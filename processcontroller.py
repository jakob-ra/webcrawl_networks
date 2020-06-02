# Scrapy uses the twister library to implement asynchronous processes. Due to the implemented non-blocking event loop,
# it's possible to run concurrent spiders. Since a web crawler is a networking application, it is an advantage to
# run multiple spiders at the same simultaneously to bridge network latency.
import sys
import csv
import os
import re

sys.path.append("../archive")
from api_crawling import ArchiveFirmWebsiteCrawler
import urllib.parse
import pandas as pd

from scrapy.crawler import CrawlerProcess
from scrapy import signals
from spider import FirmWebsiteCrawler
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
import scrapy_fake_useragent
import logging
from twisted.internet.defer import inlineCallbacks

INPUT_PATH = sys.argv[1]
OUTPUT_PATH = sys.argv[2]
PAGE_COUNT_LIMIT = int(sys.argv[3])
ACCEPTED_LANGUAGE = sys.argv[4]
DEPTH_PER_DOMAIN = int(sys.argv[5])
GET_ARCHIVE_URLS_ENABLED = sys.argv[6]
ARCHIVE_INPUT_FILE_PATH = sys.argv[7]
ARCHIVE_OUTPUT_FILE_PATH = sys.argv[8]
ARCHIVE_NAME_URL_LIST = sys.argv[9]

# Number of crawlers in a batch
MAX_BATCH_SIZE = 20

#Depending on settings.json go into archive modus or normal modus
if GET_ARCHIVE_URLS_ENABLED == "False":

# Read list of domains from file  for this process
# Read the list of url's form file passed in
    with open(INPUT_PATH, 'r') as f:
        reader = csv.reader(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
        domains = list(reader)
    f.close()

# Go into ARCHIVE MODUS
else:
    # Crawl Urls of input file first from archive and write all archive_urls into files
    archive_crawler = ArchiveFirmWebsiteCrawler(ARCHIVE_INPUT_FILE_PATH, ARCHIVE_OUTPUT_FILE_PATH, ARCHIVE_NAME_URL_LIST)
    archive_crawler.crawl_archive_urls()

    #then set the output file of archive crawler as input file of spider

    # To test webarchive urls, insert in "NAME_OF_YOUR_TEST_FILE" your test-file and uncomment!
    # with open(os.path.join(ARCHIVE_OUTPUT_FILE_PATH, NAME_OF_YOUR_TEST_FILE), 'r') as f:

    with open(os.path.join(ARCHIVE_OUTPUT_FILE_PATH, ARCHIVE_NAME_URL_LIST), 'r') as f:
        reader = csv.reader(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
        domains = list(reader)
    f.close()

# remove header entry from list
domains.pop(0)

##define logging
LOG_FILE = "crawler.log"
if LOG_FILE in os.listdir(os.getcwd()):
    os.remove(LOG_FILE)
with open(LOG_FILE,"w") as fil:
    writer = csv.writer(fil)
    writer.writerow(["response","bvdid","url"])
    
# Instantiate crawler process with a settings object
process = CrawlerProcess({

    # See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
    # https://github.com/scrapy/scrapy/blob/master/scrapy/downloadermiddlewares/robotstxt.py
    # None: disables built-in middleware
    'DOWNLOADER_MIDDLEWARES': {
        'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': None
        # 'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 400
        # 'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': None,
        # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        # 'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400
        # 'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': None
    },

    ##if not using depth: uncomment
    # 'SPIDER_MIDDLEWARES': {
    #     'scrapy.spidermiddleware.depth.DepthMiddleware': None
    # },

    'DEPTH_LIMIT': DEPTH_PER_DOMAIN,##100

    # TODO: rotate your user agent from a pool of well-known ones from browsers
    'USER_AGENT': # 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) ' \
                  #   + 'Gecko/20100101 ' \
                  #   + 'Firefox/55.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.96 Safari/537.36 Firefox/55.0 Gecko/20100101', #SemrushBot

    'DEFAULT_REQUEST_HEADERS': {
                                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                                    'Accept-Language': ACCEPTED_LANGUAGE,
                                },

    # Crawl considerate
    # TODO: Decide if robotstxt should be obeyed
    'ROBOTSTXT_OBEY': True,
    'RANDOMIZE_DOWNLOAD_DELAY': True,
    'AUTOTHROTTLE_ENABLED': True,
    'DOWNLOAD_TIMEOUT': 20, ##5,
    'DOWNLOAD_MAXSIZE': 53554432, ##~50MB; 1073741824 (1024MB)

    # TODO: Remove caching for use in production
    'HTTPCACHE_ENABLED': True,
    'DNSCACHE_ENABLED': True,

    'CONCURRENT_REQUESTS': 100, # Default: 16
    'REACTOR_THREADPOOL_MAXSIZE': 30, # Default: 10
    'CONCURRENT_REQUESTS_PER_DOMAIN': 20, # Default: 8

    'COOKIES_ENABLED': False,
    'RETRY_TIMES': 4,##potentially increase to increase prob. of successful request
    'REDIRECT_ENABLED': True,
    'AJAXCRAWL_ENABLED': True,

    # Implement breadth first crawling
    ## https://docs.scrapy.org/en/latest/topics/settings.html#depth-priority
    'DEPTH_PRIORITY': 1,
    'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',

    # Logging
    'LOG_LEVEL': 'WARNING'
})


def set_next_spider(domain):
    #INPUT FILE SHOULD BE IN THE FOLLOWING FORMAT:
    # "companyname";"city";"postcode";"streetnobuildingetcline1";"websiteaddress";"bvdidnumber";"country"
    domain_id = domain[0]
    bvdid = domain[5]
    domain_name = domain[4]
    # The prefix (e.g. http:// or http://www.) needs to be added depending on input data
    if re.search("www[.]", domain_name) is None:
        domain_name = "www."+domain_name
    if re.search("http:", domain_name) is None:
        domain_name = 'http://' + domain_name
    start_url = domain_name

    # if re.search("www[.]", domain_name) is None:
    #     domain_name = "www."+domain_name
    # if re.search("http:", domain_name) is None:
    #     domain_name = 'http://' + domain_name
    # start_url = domain_name


    # Create a directory for this domain
    ##slug_domain_to_crawl = urllib.parse.quote(domain_name, '')
#    output_dir = OUTPUT_PATH + bvdid + "/" ##slug_domain_to_crawl + '--' + domain_id + '/'
    if GET_ARCHIVE_URLS_ENABLED == "False":
        output_dir = OUTPUT_PATH + domain_id + "/" ##slug_domain_to_crawl + '--' + domain_id + '/'
        output_dir_html = output_dir + '/html/'
    else:
        output_dir = OUTPUT_PATH + domain_id + "/" + domain[7] + "/"
        output_dir_html = output_dir + '/html/'
    try:
        if not os.path.exists(output_dir_html):
            os.makedirs(output_dir_html)
    except OSError:            
        print("Error creating: %s" % output_dir)
    else:        
        print("Success creating: %s " % output_dir)

    # Create a log file for http headers
    header_log_file_name = output_dir + 'header_log.csv'
    with open(header_log_file_name, 'w') as header_csv:
        fieldnames = ['url', 'status', 'header']
        writer = csv.DictWriter(header_csv,
                                fieldnames=fieldnames,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
    header_csv.close()

    # Create a csv file to store links
    link_csv_file_name = output_dir + 'links.csv'
    with open(link_csv_file_name, 'w') as link_csv:
        fieldnames = ['origin', 'target', 'text']
        writer = csv.DictWriter(link_csv,
                                fieldnames=fieldnames,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
    link_csv.close()

    # Create a csv file to store text (bvdid added)
    text_csv_file_name = output_dir + 'text.csv'
    with open(text_csv_file_name, 'w') as text_csv:
        fieldnames = ['bvdid', 'url', 'text', 'tag', 'depth', 'path']
        writer = csv.DictWriter(text_csv,
                                fieldnames=fieldnames,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
    text_csv.close()

    process.crawl(FirmWebsiteCrawler,
                  start_url,
                  domain_name,
                  output_dir,
                  link_csv_file_name,
                  text_csv_file_name,
                  header_log_file_name,
                  PAGE_COUNT_LIMIT,
                  bvdid,
                  DEPTH_PER_DOMAIN,
                  LOG_FILE
    )

    # Add event listeners to crawlers
    ##next(iter(process.crawlers)).signals.connect(replace_finished_spider,
    #signal=signals.spider_closed)


# Start a batch of concurrent spiders
def set_next_batch(domains):
    spiders_in_batch = min(MAX_BATCH_SIZE, len(domains))
    for d in domains[:spiders_in_batch]:
        set_next_spider(d)
    if spiders_in_batch<MAX_BATCH_SIZE:
        return 0
    else:
        return set_next_batch(domains[spiders_in_batch:])


##Start first spider batch
set_next_batch(domains)
# set_next_spider([1,"test","www.pashagroup.com"])
# for d in domains[s:e]:
#     set_next_spider(d)
process.start()
