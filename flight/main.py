from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from flight_project.spiders import AirportsSpider

#from scrapy import cmdline
import subprocess
import pendulum

print("RUN SCRAPY RUN ! ")

configure_logging()
runner = CrawlerRunner()

@defer.inlineCallbacks
def crawl():
    yield runner.crawl(AirportsSpider.AirportsSpider)
    reactor.stop()

crawl()
reactor.run()


## PYMONGO DUMP

subprocess.run("mongodump --host mongodb_service:27017 --archive=/home/scrapy/backup/{timestamp}.gz --gzip".format(timestamp=pendulum.now()), shell=True, check=True)

subprocess.run("mongo --host mongodb_service:27017 flight_project --eval 'db.airports.drop();'", shell=True, check=True)

