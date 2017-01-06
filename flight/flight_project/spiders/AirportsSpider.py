import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import Rule
from flight_project.items.Items import *
from bs4 import BeautifulSoup



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

    def parse(self, response):

        self.logger.info("Visited main page %s", response.url)
        for country in response.xpath('//a[@data-country]'):
            item = CountryItem()
            url =  country.xpath('./@href').extract()
            name = country.xpath('./@title').extract()
            item['link'] = url[0]
            item['name'] = name[0]
            self.logger.info("Airport name : %s with link %s" , item['name'] , item['link'])
            yield scrapy.Request(url[0],meta={'my_country_item':item}, callback=self.parse_page2)


        #return item

    def parse_page2(self,response):
        item = response.meta['my_country_item']
        self.logger.info("Airport name : %s", item['name'])
        self.logger.info("Visited airport page %s", response.url)
        airports = []

        for airport in response.xpath('//a[@data-iata]'):
            url = airport.xpath('./@href').extract()
            iata = airport.xpath('./@data-iata').extract()
            iatabis = airport.xpath('./small/text()').extract()
            name = ''.join(airport.xpath('./text()').extract()).strip()
            lat = airport.xpath("./@data-lat").extract()
            lon = airport.xpath("./@data-lon").extract()
            self.logger.info("Name = %s", name)
            iAirport = AirportItem()
            iAirport['name'] = self.clean_html(name)
            iAirport['link'] = url[0]
            iAirport['lat'] = lat[0]
            iAirport['lon'] = lon[0]
            iAirport['code_little'] = iata[0]
            iAirport['code_total'] = iatabis[0]

            airports.append(iAirport)

        item['airports'] = airports

        return item

