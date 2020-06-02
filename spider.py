import scrapy
from scrapy.selector import Selector
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from selectolax.parser import HTMLParser

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

import urllib.parse
import csv
import shutil
import re
from urllib.parse import urlparse
import os
import time
import logging
from scrapy.utils.log import configure_logging


"""
important remark:
I commented all lines that write to a file,
except for links and text
"""


# Define crawler #
class FirmWebsiteCrawler(scrapy.Spider):

    # Scrapy crawler settings
    name = 'firm-website-crawler'
    page_count = 0

    # Parse parameters of crawler
    def __init__(self, start_url,
                 allowed,
                 output_dir,
                 link_csv_file_name,
                 text_csv_file_name,
                 header_log_file_name,
                 page_count_limit,
                 bvdid,
                 max_depth,
                 logfile,
                 *args, **kwargs):
        
        ##clean domain urls for re matching with links
        ##assumption: both inputs are strings (not lists!)
        self.start_urls = [start_url]
        self.allowed_domains = [re.sub(".*www[.]", "", allowed)]

        self.output_dir = output_dir
        self.output_dir_html = self.output_dir + 'html/'

        self.link_csv_file_name = link_csv_file_name
        self.text_csv_file_name = text_csv_file_name
        self.header_log_file_name = header_log_file_name

        self.page_count_limit = page_count_limit
        self.bvdid = bvdid
        self.max_depth = max_depth
        self.langcheck = False
        self.logfile = logfile
        self.parsedLinks = []

        super().__init__(*args, **kwargs)       
        
        
    ##https://github.com/scrapy/scrapy/blob/master/docs/topics/spiders.rst#id19
    ##to-do: more validation checks needed!
    ##microsoft.com/en returns page saying "url not available"
    ##(e.g. compare approx. number of links)
    def start_requests(self):
        for link in self.start_urls:
            yield scrapy.Request(link,errback=self.errback,callback=self.parse,dont_filter=False,meta={"depth":1})
                   

    # Called on 'close signal' of crawler (shortcut to signals.connect())
    def closed(self, reason):
        pass
        # zip html
        # self.zip_html_folder()

    # Write http header log file
    def write_log(self, response):

        # Write links to csv
        with open(self.header_log_file_name, 'a', encoding='utf8') as log_file:
            writer = csv.writer(log_file,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL)

            csv_row = [response.url, response.status, response.headers]
            writer.writerow(csv_row)
        log_file.close()


    # Save the received html
    def save_html(self, response):

        filename = self.output_dir_html + urllib.parse.quote(response.url, '')[0:249] + '.html'
        # Parsing can be reversed with urllib.parse.unquote(string), '')

        with open(filename, 'wb') as html_file:
            html_file.write(response.body)
        html_file.close()

    # Zip the stored html files
    def zip_html_folder(self):

        html_zip_file_path = self.output_dir + 'html'

        shutil.make_archive(html_zip_file_path
                            , 'zip'
                            , self.output_dir_html)

        shutil.rmtree(self.output_dir_html)
        
    # Extract and save links
    def save_links(self, response):

        # Write links to csv
        with open(self.link_csv_file_name, 'a', encoding='utf8') as link_file:
            writer = csv.writer(link_file,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL)

            for link in LxmlLinkExtractor().extract_links(response):
                csv_row = [response.url, link.url, " ".join(link.text.strip().split())]
                writer.writerow(csv_row)

        link_file.close()

    # Extract and save text
    def save_text(self, response):

        ##ignore encoding errors
        tree = HTMLParser(str(response.body, errors="ignore"))

        if tree.html is None:
            return None

        # remove script and style tags
        for tag in tree.css('script'):
            tag.decompose()

        for tag in tree.css('style'):
            tag.decompose()

        with open(self.text_csv_file_name, 'a', encoding="utf-8") as text_file:
            writer = csv.writer(text_file,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL,
								)

            for node in tree.css('*'):

                if node.text(strip=True,
                             separator='',
                             deep=False) != '':

                    node_path = self.get_node_path(node, node.tag, 0)


                    csv_row = [self.bvdid,
                               response.url,
                               " ".join(node.text(strip=True,
                                                  separator=' ',
                                                  deep=False).split()),
                               node.tag,
                               node_path[0],
                               node_path[1]]

                    writer.writerow(csv_row)

        text_file.close()

    # Get the path and depth of nodes recursively
    def get_node_path(self, node, path, path_length):

        if node.parent and node.parent.tag != '-undef':
            path_length = path_length + 1
            path = node.parent.tag + ' ' + path

            return self.get_node_path(node.parent,
                                      path,
                                      path_length)

        else:
            return [path_length, path]

        
    ##function for error handling
    ##http://doc.scrapy.org/en/latest/topics/request-response.html#topics-request-response-ref-errbacks
    def errback(self, failure):
        if type(failure)==str:
            if failure=="lang":
                err = "NoEnglishLang"
                response = "NoEnglishLang"
        else:
            if failure.check(HttpError):
                response = failure.value.response
                err = "HttpError "+str(response.status)
            elif failure.check(DNSLookupError):
                response = failure.request
                err = "DNSLookupError"
            elif failure.check(TimeoutError, TCPTimedOutError):
                response = failure.request
                err = "TimeoutError"
            else:
                response = failure.request
                err = "UnknownError"
        with open(self.logfile,"a") as f:
            writer = csv.writer(f)
            writer.writerow([response, self.bvdid, self.start_urls[0]])
        ##write error to csv file
        with open(self.text_csv_file_name, 'a', encoding="utf-8") as text_file:
            writer = csv.writer(text_file,
                                delimiter=';',
                                quotechar="'",
                                quoting=csv.QUOTE_ALL)
            csv_row = [self.bvdid, self.start_urls[0], err, "", "", ""]
            writer.writerow(csv_row)
        text_file.close()

        
    # Parsing of pages - do not overwrite (scrapy's parse function must not be overwritten)
    def parse(self, response):

        if response.url in self.parsedLinks or response.meta["depth"]>self.max_depth:
            return
        ##append response.url to not follow duplicated links
        self.parsedLinks.append(response.url)

        ##verify that domain's language is english!
        if not self.langcheck:
            try:
                res = Selector(response)
                langtemp = res.xpath("//html/@lang").extract()
                lang = langtemp[0]
                if re.search("en",lang,re.IGNORECASE) is not None or lang=="":
                    self.langcheck = True
                    pass
                elif re.search("en",lang,re.IGNORECASE) is None:
                    self.errback("lang")
                    return 0
            ##if does not find html/@lang, assume language is en 
            except:
                self.langcheck = True
                pass

        ##TO-DO:
		##make allowed_domains more flexible from main page of each domain
        ##i.e. orbis_webaddresses is different from realized start_url (see
        ##cardinal health or kpmg: home.kpmg/)
        if self.page_count==0:
            self.allowed_domains = self.allowed_domains + [re.sub("www[.]","",urlparse(response.url).netloc)]

        # self.write_log(response)

        # Check if response is html
        if 'text/html' in response.headers.get("content-type","").decode('utf-8').lower():

            # Check if page limit for this domain is exceeded
            if self.page_count_limit >= self.page_count:

                    #Write "header file"
                    self.write_log(response)

                    # Save html file
                    # self.save_html(response)

                    # Extract and save links
                    self.save_links(response)

                    # Extract and save text
                    self.save_text(response)

                    # Saved page count is updated after saving content
                    self.page_count += 1

            # Check if page limit for this domain is exceeded
            if self.page_count_limit > self.page_count:
                # Search for links within domain
                ##assumption *CONTAINS* domain!
                pat = "("+"|".join(self.allowed_domains)+")"
                links = LxmlLinkExtractor(allow=pat).extract_links(response)
                for link in links:
                    # TODO: Lists could be sorted by the count of their /fragments/
                    #  -> this would support the breadth first search, which is implemented via scrapy settings

                    # Estimate how many links there will be used still to satisfy the required page count
                    # This should prevent an excess of requested pages
                    ##if self.page_count_limit * 2 > idx:
                    ## stop if max depth is reached
                    ##searchQ = re.sub(".+"+self.allowed_domains[0], "", link.url)
                    # if re.findall("[^/]+", searchQ).__len__() <= self.max_depth:
                    if link.url not in self.parsedLinks:
                        yield scrapy.Request(link.url,callback=self.parse,errback=self.errback,dont_filter=True,meta={"depth":response.meta["depth"]+1})
                        
            else:
                self._stop_following_links = True
                self.crawler.engine.close_spider(self, 'Spider completed page limit')


