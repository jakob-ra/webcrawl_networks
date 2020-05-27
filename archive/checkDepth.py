import requests
import pandas as pd
import os
import csv
import re
from urllib.parse import urlparse
import json
import sys
import numpy as np
import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse
from twisted.internet.error import ConnectionRefusedError
requests.adapters.DEFAULT_RETRIES = 10


##define spider class
class FollowInternalLinks(scrapy.Spider):
    def __init__(self,start_url,orbisweb,bvdid,year,filename):
        self.start_url = start_url
        self.nlinks = 0
        self.success = 0
        self.bvdid = bvdid
        self.year = year
        self.allow = orbisweb
        self.filename = filename
    def start_requests(self):
        yield scrapy.Request(self.start_url,callback=self.parse)
    def parse(self,response):
        ##response only from body
        # html = requests.get(response,timeout=40)
        # bs = BeautifulSoup(html.text)
        # body = bs.find("body")
        # print(response.url,len(response.body))
        # responseb = HtmlResponse(url=html.url,body=str(body),encoding="utf-8")
        responseb = HtmlResponse(url=response.url, body=response.body)
        linkObjs = LxmlLinkExtractor().extract_links(responseb)
        ##include(?):re.search("(([.]css|;)$|javascript|mailto:|tel:)",i) is None
        ##keep links orbisweb (not optimal yet!)
        ##include %s/ ?
        pattern = "([.]%s|%s[.])" % (self.allow,self.allow)
        links = [l.url for l in linkObjs if re.search(pattern,l.url) is not None]
        links = list(set(links))
        self.nlinks = len(links)
        for l in links:
            #self.successParse(l)
            yield self.successParse(l)
            # yield scrapy.Request(l,callback=self.successParse,errback=self.errback,dont_filter=True)
    def errback(self,failure):
        if failure.check(ConnectionRefusedError):
            print("ConnectionRefusedError:",failure.response.url)
        else:
            print(failure.request,failure.response.url)
    def successParse(self,response):
        try:
            ##on ssl error: https://stackoverflow.com/questions/10667960/python-requests-throwing-sslerror
            check = requests.get(re.sub("^https","http",response),timeout=40)#.url
            ##to-do: check year of check
            if check.status_code==200:
                self.success += 1
        except Exception as e:
            print(e, response)#.url


class LinkPipeline(object):
    def close_spider(self,spider):
        with open(spider.filename, 'a') as out:
            fieldnames = [spider.bvdid, spider.year, spider.nlinks, spider.success, spider.start_url]
            writer = csv.DictWriter(out,fieldnames=fieldnames,delimiter=';',quotechar="'",quoting=csv.QUOTE_ALL)
            writer.writeheader()
        out.close()

##change number of concurrent requests (and per domain) when ulimit -n is changed
##-> too many open files      
##https://stackoverflow.com/questions/36496082/scrapy-script-cannotlistenerror  
process = CrawlerProcess({'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64; rv:7.0.1) Gecko/20100101 Firefox/7.7",'DOWNLOAD_TIMEOUT':100,'REDIRECT_ENABLED':True,'BOTSTXT_OBEY':False,'DEFAULT_REQUEST_HEADERS': {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language': "*"},'LOG_LEVEL': 'ERROR','ITEM_PIPELINES':{'__main__.LinkPipeline': 300},'CONCURRENT_REQUESTS_PER_DOMAIN': 10,'CONCURRENT_REQUESTS': 100,'REACTOR_THREADPOOL_MAXSIZE': 30,'RETRY_TIMES': 100,'TELNETCONSOLE_PORT': None})


def set_next_spider(url,orbisweb,bvdid,year,filename):
    process.crawl(FollowInternalLinks,
                  start_url=url,
                  orbisweb=orbisweb,
                  bvdid=bvdid,
                  year=year,
                  filename=filename
    )


##read data
nproc = int(sys.argv[1])-1
firms = os.listdir(os.getcwd()+"/url-company-data")
firms = [f for f in firms if f.endswith(".csv")]
NPROCS = 8
STEPSIZE = int(np.ceil(len(firms) / NPROCS))
start = nproc*STEPSIZE
end = (nproc+1)*STEPSIZE
firms = firms[start:end]
firmDataOrbis = pd.read_csv("Orbis_Web_CH.csv",";")
firmDataOrbis["bvdidnumber"] = [str(i).lower() for i in firmDataOrbis["bvdidnumber"]]
firmDataOrbis["websiteaddress"] = [str(i) for i in firmDataOrbis["websiteaddress"]]

for firm in firms[:100]:
    tempurl = firmDataOrbis[firmDataOrbis["bvdidnumber"]==firm.replace(".csv","")]
    ##example: kpmg
    orbisweb = re.findall("(?:www.)?([^.]+)[.]",tempurl["websiteaddress"].iloc[0])[0]
    print(firm,orbisweb)
    if firm in os.listdir(os.getcwd()+"/url-company-depth"):
        continue
    ##output file
    with open(os.getcwd()+"/url-company-depth/"+firm, 'w') as out:
        fieldnames = ['bvdid', 'year', 'nlinks', 'success', 'url']
        writer = csv.DictWriter(out,fieldnames=fieldnames,delimiter=';',quotechar="'",quoting=csv.QUOTE_ALL)
        writer.writeheader()
    out.close()
    with open(os.getcwd()+"/url-company-data/"+firm,"r") as f:
        firmdata = json.load(f)
    f.close()
    firmurls = firmdata["url"]
    inputs = []
    ##list of tuples with url and year (only links with clear year identification)
    for url in firmurls:
        try:
            inputs.append((url,re.findall("/([0-9]{8,})/",url)[0][:4]))
        except:
            continue
    output = []
    for ii in range(len(inputs)):
        html = None
        url = inputs[ii][0]
        year = inputs[ii][1]
        ##check depth on yearly basis
        if year in output:
            continue
        ##add to spider
        else:
            ##if webpage saving with errors, use request for automatic
            ##correct version (auto refresh on web.archive.org)
            ##e.g. https://web.archive.org/web/20190117070109/http://www.kpmg.ch/
            try:
                html = requests.get(url,timeout=40)
                if html.status_code==200:
                    url = html.url
                    ##check whether same year
                    try:
                        yearURL = re.findall("/([0-9]{8,})/",url)[0][:4]
                    except:
                        continue
                    if yearURL!=year:
                        continue
                    output.append(year)
                    print("...start spider")
                    set_next_spider(url,orbisweb,firm.replace(".csv",""),year,os.getcwd()+"/url-company-depth/"+firm)
                else:
                    ##to-do: store info if nothing in year is correctly
                    ##stored, e.g. merge with url-company-data
                    continue
            ##if webpage cannot be reached, or no status_code 
            except:# ConnectionRefusedError:
                continue
process.start()




















