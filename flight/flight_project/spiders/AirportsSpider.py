import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import Rule
from flight_project.items.Items import *
from bs4 import BeautifulSoup
import cfscrape
import json
import jmespath
from fake_useragent import UserAgent

class AirportsSpider(scrapy.Spider):
    name = "airports"
    start_urls = ['https://www.flightradar24.com/data/airports']
    allowed_domains = ['flightradar24.com']

    ua = UserAgent()

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

    def compute_timestamp(self):
        from datetime import datetime, date
        import calendar
        # +/- 24 heures
        d = date(2017, 4, 27)
        timestamp = calendar.timegm(d.timetuple())
        return timestamp

    def build_api_call(self,code,page,timestamp):
        return 'https://api.flightradar24.com/common/v1/airport.json?code={code}&plugin\[\]=&plugin-setting\[schedule\]\[mode\]=&plugin-setting\[schedule\]\[timestamp\]={timestamp}&page={page}&limit=100&token='.format(
            code=code, page=page, timestamp=timestamp)

    ###################################
    # MAIN PARSE
    ####################################
    def parse(self, response):
        count_country = 0
        countries = []
        #self.logger.info("Visited main page %s", response.url)
        for country in response.xpath('//a[@data-country]'):
            item = CountryItem()
            url =  country.xpath('./@href').extract()
            name = country.xpath('./@title').extract()
            item['link'] = url[0]
            item['name'] = name[0]
            item['airports'] = []
            count_country += 1
            if name[0] == "Israel":
                countries.append(item)
                self.logger.info("Airport name : %s with link %s" , item['name'] , item['link'])
                yield scrapy.Request(url[0],meta={'my_country_item':item}, callback=self.parse_airports)

    ###################################
    # PARSE EACH AIRPORT
    ####################################
    def parse_airports(self, response):
        item = response.meta['my_country_item']
        item['airports'] = []

        for airport in response.xpath('//a[@data-iata]'):
            url = airport.xpath('./@href').extract()
            iata = airport.xpath('./@data-iata').extract()
            iatabis = airport.xpath('./small/text()').extract()
            name = ''.join(airport.xpath('./text()').extract()).strip()
            lat = airport.xpath("./@data-lat").extract()
            lon = airport.xpath("./@data-lon").extract()
            iAirport = AirportItem()
            iAirport['name'] = self.clean_html(name)
            iAirport['link'] = url[0]
            iAirport['lat'] = lat[0]
            iAirport['lon'] = lon[0]
            iAirport['code_little'] = iata[0]
            iAirport['code_total'] = iatabis[0]

            item['airports'].append(iAirport)

        urls = []
        for airport in item['airports']:
            json_url = self.build_api_call(airport['code_little'], 1, self.compute_timestamp())
            urls.append(json_url)
        if not urls:
            return item

        # start with first url
        next_url = urls.pop()
        return scrapy.Request(next_url, self.parse_schedule, meta={'airport_item': item, 'airport_urls': urls, 'i': 0})


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
            urls_departures.append(self.build_api_call(airport_code, p + 1 , self.compute_timestamp()))

        for p in range(nbpages_arrivals):
            urls_arrivals.append(self.build_api_call(airport_code, p + 1, self.compute_timestamp()))

        return urls_departures, urls_arrivals

    ###################################
    # PARSE EACH DEPARTURES / ARRIVALS
    ###################################
    def parse_departures_page(self, response):
        item = response.meta['airport_item']
        p = response.meta['p']
        i = response.meta['i']
        page_urls = response.meta['page_urls']

        print("PAGE URL = ", page_urls)

        if not page_urls:
            yield item
            return
        page_url = page_urls.pop()

        print("GET PAGE FOR  ", item['airports'][i]['name'], ">> ", p)

        jsonload = json.loads(response.body_as_unicode())
        json_expression = jmespath.compile("result.response.airport.pluginData.schedule.departures.data")
        item['airports'][i]['departures'] = json_expression.search(jsonload)

        yield scrapy.Request(page_url, self.parse_departures_page, meta={'airport_item': item, 'page_urls': page_urls, 'i': i, 'p': p + 1})

    ###################################
    # PARSE EACH AIRPORT OF COUNTRY
    ###################################
    def parse_schedule(self, response):
            """we want to loop this continuously to build every departure and arrivals requests"""
            item = response.meta['airport_item']
            i = response.meta['i']
            urls = response.meta['airport_urls']

            urls_departures, urls_arrivals = self.compute_urls_by_page(response, item['airports'][i]['name'], item['airports'][i]['code_little'])

            print("urls_departures = ", len(urls_departures))
            print("urls_arrivals = ", len(urls_arrivals))

            ## GET EACH DEPARTURE PAGE

            #jsonload = json.loads(response.body_as_unicode())
            #json_expression = jmespath.compile("result.response.airport.pluginData.schedule")
            #item['airports'][i]['schedule'] = json_expression.search(jsonload)

            print("GET DEPARTURES URL - START FOR ", item['airports'][i]['name'])
            yield scrapy.Request(response.url, self.parse_departures_page, meta={'airport_item': item, 'page_urls': urls_departures, 'i':0 , 'p': 0})
            print("GET DEPARTURES URL - END FOR " , item['airports'][i]['name'])

            # now do next schedule items
            if not urls:
                yield item
                return
            url = urls.pop()

            yield scrapy.Request(url, self.parse_schedule, meta={'airport_item': item, 'airport_urls': urls, 'i': i + 1})

    # def parse_airports(self, response):
    #     item = response.meta['my_country_item']
    #     airports = []
    #
    #     for airport in response.xpath('//a[@data-iata]'):
    #         url = airport.xpath('./@href').extract()
    #         iata = airport.xpath('./@data-iata').extract()
    #         iatabis = airport.xpath('./small/text()').extract()
    #         name = ''.join(airport.xpath('./text()').extract()).strip()
    #         lat = airport.xpath("./@data-lat").extract()
    #         lon = airport.xpath("./@data-lon").extract()
    #         iAirport = AirportItem()
    #         iAirport['name'] = self.clean_html(name)
    #         iAirport['link'] = url[0]
    #         iAirport['lat'] = lat[0]
    #         iAirport['lon'] = lon[0]
    #         iAirport['code_little'] = iata[0]
    #         iAirport['code_total'] = iatabis[0]
    #         airports.append(iAirport)
    #
    #     item['num_airports'] = 0
    #     item['airports'] = airports
    #     for airport in airports:
    #         json_url = 'https://api.flightradar24.com/common/v1/airport.json?code={code}&plugin\[\]=&plugin-setting\[schedule\]\[mode\]=&plugin-setting\[schedule\]\[timestamp\]={timestamp}&page=1&limit=50&token='.format(
    #             code=airport['code_little'], timestamp="1484150483")
    #         yield scrapy.Request(json_url,
    #                              meta={
    #                                  'country_item': item,
    #                                  'airport_item': airport,
    #                                  'max_num_airports': len(airports)},
    #                              callback=self.parse_schedule)
    #
    #     if not airports:
    #         yield item
    #
    # def parse_schedule(self, response):
    #     country_item = response.request.meta['country_item']
    #     airport_item = response.request.meta['airport_item']
    #     jsonload = json.loads(response.body_as_unicode())
    #     json_expression = jmespath.compile("result.response.airport.pluginData.schedule")
    #     airport_item['schedule'] = json_expression.search(jsonload)
    #
    #     country_item['num_airports'] += 1
    #     if country_item['num_airports'] == response.request.meta['max_num_airports']:
    #         del country_item['num_airports']
    #         yield country_item