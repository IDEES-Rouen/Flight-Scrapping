from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scrapy import cmdline
import subprocess

#cmdline.execute("scrapy crawl airports".split())

# https://doc.scrapy.org/en/latest/topics/practices.html
#process = CrawlerProcess(get_project_settings())
#process.crawl('airports')
#process.start()

## PYMONGO DUMP

#from pymongo import MongoClient
#client = MongoClient('mongodb://mongodb_service:27017/')

# permission denied sur archive option ? test out ?
subprocess.run("ls -l /",shell=True, check=True)

subprocess.run("ls -l /backup/",shell=True, check=True)

subprocess.run("nmap -p 27017 mongodb_service",shell=True, check=True)

subprocess.run("mongodump --host mongodb_service:27017 --archive=/backup/test.2017.gz --gzip", shell=True, check=True)

subprocess.run("ls -l /backup/", shell=True, check=True)
