import scrapy

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

#from scrapy import cmdline
import subprocess
import pendulum

#cmdline.execute("scrapy crawl airports".split())

#https://doc.scrapy.org/en/latest/topics/practices.html
process = CrawlerProcess(get_project_settings())
process.crawl('airports')
process.start()

## PYMONGO DUMP

subprocess.run("mongodump --host mongodb_service:27017 --archive=/home/scrapy/backup/{timestamp}.gz --gzip".format(timestamp=pendulum.now()), shell=True, check=True)

subprocess.run("mongo --host mongodb_service:27017 flight_project --eval 'db.airports.drop();'", shell=True, check=True)

