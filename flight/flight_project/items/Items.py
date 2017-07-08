# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class PageItem(scrapy.Item):
    name = scrapy.Field()
    flight = scrapy.Field()
    arrivalsPFlight = scrapy.Field()
    departuresPFlight = scrapy.Field()
    currentAPage = scrapy.Field()
    totalAPage = scrapy.Field()
    currentDPage = scrapy.Field()
    totalDPage = scrapy.Field()

class CountryItem(scrapy.Item):
    name = scrapy.Field()
    link = scrapy.Field()
    num_airports = scrapy.Field()
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
    pages = scrapy.Field()

class ScheduleItem(scrapy.Item):
    country = scrapy.Field()
    airport = scrapy.Field()
    timestamp = scrapy.Field()