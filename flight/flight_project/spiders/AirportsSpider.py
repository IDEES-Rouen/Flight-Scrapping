import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import Rule
from flight_project.items.Items import *
from bs4 import BeautifulSoup
import cfscrape
import json
import jmespath
from fake_useragent import UserAgent
import pendulum

class AirportsSpider(scrapy.Spider):
    name = "airports"
    start_urls = ['https://www.flightradar24.com/data/airports']
    allowed_domains = ['flightradar24.com']
    ua = UserAgent()

    def __init__(self, aDate = pendulum.today()):
        super(AirportsSpider, self).__init__()
        self.aDate = aDate
        self.timestamp = self.aDate.timestamp()
        print("PENDULUM UTC TODAY ", self.aDate.isoformat())
        print("PENDULUM TO TIMESTAMP ", self.timestamp)
    def clean_html(self, html_text):
        soup = BeautifulSoup(html_text, 'html.parser')
        return soup.get_text()

    rules = [
    # Extract links matching 'item.php' and parse them with the spider's method parse_item
        Rule(LxmlLinkExtractor(allow=('data/airports/',)), callback='parse')
    ]

    def start_requests(self):
        cf_requests = []
        user_agent = self.ua.random
        self.logger.info("RANDOM user_agent = %s", user_agent)
        for url in self.start_urls:
            token , agent = cfscrape.get_tokens(url,user_agent)
            self.logger.info("token = %s", token)
            self.logger.info("agent = %s", agent)

            cf_requests.append(scrapy.Request(url=url,
                                              cookies= token,
                                              headers={'User-Agent': agent}))
        return cf_requests

    def build_api_call(self,code,page,timestamp):
        return 'https://api.flightradar24.com/common/v1/airport.json?code={code}&plugin\[\]=&plugin-setting\[schedule\]\[mode\]=&plugin-setting\[schedule\]\[timestamp\]={timestamp}&page={page}&limit=100&token='.format(
            code=code, page=page, timestamp=timestamp)

    ###################################
    # MAIN PARSE
    ####################################

    def parse(self, response):

        for a_country in response.xpath('//a[@data-country]'):
            name = a_country.xpath('./@title').extract()[0]
            if name == "France":
                country = CountryItem()
                country['name'] = name
                country['link'] = a_country.xpath('./@href').extract()[0]

                yield scrapy.Request(country['link'],
                                     meta={'country': country},
                                     callback=self.parse_airports)

    ###################################
    # PARSE EACH AIRPORT
    ####################################
    def parse_airports(self, response):
        country = response.meta['country']

        for airport in response.xpath('//a[@data-iata]'):
            name = ''.join(airport.xpath('./text()').extract()[0]).strip()

         #   if 'Charles' in name:
            meta = response.meta
            airportI = AirportItem()
            meta['airport'] = airportI
            meta['airport']['name'] = name
            meta['airport']['link'] = airport.xpath('./@href').extract()[0]
            meta['airport']['lat'] = airport.xpath("./@data-lat").extract()[0]
            meta['airport']['lon'] = airport.xpath("./@data-lon").extract()[0]
            meta['airport']['code_little'] = airport.xpath('./@data-iata').extract()[0]
            meta['airport']['code_total'] = airport.xpath('./small/text()').extract()[0]
            meta['airport']['pages'] = []

            json_url = self.build_api_call(meta['airport']['code_little'], 1, self.timestamp)
            yield scrapy.Request(json_url, meta=meta, callback=self.parse_schedule)

    ###################################
    # PARSE EACH AIRPORT OF COUNTRY
    ###################################
    def parse_schedule(self, response):
        meta = response.meta

        if not 'schedule' in meta:
            # First call from parse_airports
            schedule = ScheduleItem()
            schedule['timestamp'] = self.timestamp
            schedule['country'] = response.meta['country']
            schedule['airport'] = response.meta['airport']
        else:
            schedule = response.meta['schedule']

        data = json.loads(response.body_as_unicode())
        airport = data['result']['response']['airport']

        # FIRST PAGE IF EXIST ?

        arrivals_from_json = airport['pluginData']['schedule']['arrivals']
        departures_from_json = airport['pluginData']['schedule']['departures']

        # Number of departures and arrivals are equivalent most of time in airports,
        # so there is only one page parameters into query to json
        if departures_from_json['page']['total'] != 0 or arrivals_from_json['page']['total'] != 0:
            # CREATE FIRST PAGEITEM ONLY IF PAGE EXIST
            pi = PageItem()
            pi['totalDPage'] = departures_from_json['page']['total']
            pi['totalAPage'] = arrivals_from_json['page']['total']

            if departures_from_json['page']['total'] != 0 :
                pi['currentDPage'] = departures_from_json['page']['current']
                pi['departuresPFlight'] = departures_from_json
            else:
                pi['currentDPage'] = 0
            if arrivals_from_json['page']['total'] != 0 :
                pi['currentAPage'] = arrivals_from_json['page']['current']
                pi['arrivalsPFlight'] = arrivals_from_json
            else:
                pi['currentAPage'] = 0

            # TAKING ACCOUNT OF ONE DEPARTURESANDARRIVALSPAGE SUFFICE BECAUSE IT CONTAIN TWICE THE SAME INFO !
            schedule['airport']['pages'].append(pi)

            # TAKING DEPARTURE PAGE COUNTING TO CHECK/CREATE NEXT PAGE
            page = schedule['airport']['pages'][-1]

            if pi['currentDPage']  < pi['totalDPage']:
                json_url = self.build_api_call(schedule['airport']['code_little'],  pi['currentDPage'] + 1  ,
                                               self.timestamp)
                yield scrapy.Request(json_url, meta={'schedule': schedule}, callback=self.parse_schedule)
            else:
                yield schedule
        else:
            yield schedule

    def compute_urls_by_page(self,response,airport_name,airport_code):

        jsonload = json.loads(response.body_as_unicode())

        # generate expressions to catch item
        json_expression1 = jmespath.compile("result.response.airport.pluginData.schedule.departures.item.total")
        json_expression2 = jmespath.compile("result.response.airport.pluginData.schedule.arrivals.item.total")
        total_items_departures = json_expression1.search(jsonload)
        total_items_arrivals = json_expression2.search(jsonload)

        print("item_name = ", airport_name)
        print("item_name = ", airport_code)
        print("total_items_departures = ", total_items_departures)
        print("total_items_arrivals = ", total_items_arrivals)

        nbpages_departures = 1
        nbpages_arrivals = 1

        if total_items_departures//100 >= 1 :
            nbpages_departures = total_items_departures//100
            if total_items_departures > (nbpages_departures * 100):
                nbpages_departures += 1

        if total_items_arrivals//100 >= 1:
            nbpages_arrivals = total_items_arrivals // 100
            if total_items_arrivals > (nbpages_arrivals * 100):
                nbpages_arrivals += 1

        urls_departures = []
        urls_arrivals = []

        print("nbpages_departures = ", nbpages_departures)
        print("nbpages_arrivals = ", nbpages_arrivals)

        for p in range(nbpages_departures):
            urls_departures.append(self.build_api_call(airport_code, p + 1 , self.timestamp))

        for p in range(nbpages_arrivals):
            urls_arrivals.append(self.build_api_call(airport_code, p + 1, self.timestamp))

        return urls_departures, urls_arrivals
