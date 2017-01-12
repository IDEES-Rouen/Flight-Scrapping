# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class CountryItem(scrapy.Item):
    name = scrapy.Field()
    link = scrapy.Field()
    airports = scrapy.Field()
    other_url= scrapy.Field()
    last_updated = scrapy.Field(serializer=str)

class AirportItem(scrapy.Item):
    name = scrapy.Field()
    code_little = scrapy.Field()
    code_total = scrapy.Field()
    lat = scrapy.Field()
    lon = scrapy.Field()
    link = scrapy.Field()
    schedule = scrapy.Field()

class DeparturesItem(scrapy.Item):
    raw = scrapy.Field()
    origin = scrapy.Field()
    destination = scrapy.Field()