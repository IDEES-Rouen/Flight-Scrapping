import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import Rule
from flight_project.items.airportItems import AirportItem

class AirportsSpider(scrapy.Spider):
    name = "airports"
    start_urls = ['https://www.flightradar24.com/data/airports']
    allowed_domains = ['flightradar24.com']

    rules = [
    # Extract links matching 'item.php' and parse them with the spider's method parse_item
        Rule(LxmlLinkExtractor(allow=('data/airports/',)), callback='parse')
    ]

    def parse(self, response):
        item = AirportItem()
        item['name'] = response.xpath('//a[@data-country]/@title').extract()
        item['link'] = response.xpath('//a[@data-country]/@href').extract()
        return item