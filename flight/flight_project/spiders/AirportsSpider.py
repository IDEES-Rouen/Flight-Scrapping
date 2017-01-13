import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import Rule
from flight_project.items.Items import *
from bs4 import BeautifulSoup
import cfscrape
import json
import jmespath

class AirportsSpider(scrapy.Spider):
    name = "airports"
    start_urls = ['https://www.flightradar24.com/data/airports']
    allowed_domains = ['flightradar24.com']

    def clean_html(self, html_text):
        soup = BeautifulSoup(html_text, 'html.parser')
        return soup.get_text()

    rules = [
    # Extract links matching 'item.php' and parse them with the spider's method parse_item
        Rule(LxmlLinkExtractor(allow=('data/airports/',)), callback='parse')
    ]

    def start_requests(self):
        cf_requests = []
        user_agent=self.settings['USER_AGENT']
        for url in self.start_urls:
            token , agent = cfscrape.get_tokens(url)
            self.logger.info("token = %s", token)
            self.logger.info("agent = %s", agent)

            cf_requests.append(scrapy.Request(url=url,
                                              cookies= token))
        return cf_requests

    def parse(self, response):
        count_country = 0
        countries = []
        #self.logger.info("Visited main page %s", response.url)
        for country in response.xpath('//a[@data-country]'):
            if count_country > 5:
                break
            item = CountryItem()
            url =  country.xpath('./@href').extract()
            name = country.xpath('./@title').extract()
            item['link'] = url[0]
            item['name'] = name[0]
            count_country += 1
            countries.append(item)
            #self.logger.info("Airport name : %s with link %s" , item['name'] , item['link'])
            yield scrapy.Request(url[0],meta={'my_country_item':item}, callback=self.parse_airports)

    def parse_airports(self,response):
        item = response.meta['my_country_item']
        airports = []

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

            airports.append(iAirport)

        for airport in airports:
            json_url = 'https://api.flightradar24.com/common/v1/airport.json?code={code}&plugin\[\]=&plugin-setting\[schedule\]\[mode\]=&plugin-setting\[schedule\]\[timestamp\]={timestamp}&page=1&limit=50&token='.format(code=airport['code_little'], timestamp="1484150483")
            yield scrapy.Request(json_url, meta={'airport_item': airport}, callback=self.parse_schedule)

        item['airports'] = airports

        yield {"country" : item}

    def parse_schedule(self,response):

        item = response.request.meta['airport_item']
        jsonload = json.loads(response.body_as_unicode())
        json_expression = jmespath.compile("result.response.airport.pluginData.schedule")
        item['schedule'] = json_expression.search(jsonload)
        # search into json.result.response.airport.pluginData.schedule
