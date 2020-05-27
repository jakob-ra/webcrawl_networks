from time import sleep
import requests
import json
import os
import csv
import random
import re
import unicodedata

from spider import FirmWebsiteCrawler

# Define crawler #
class ArchiveFirmWebsiteCrawler():

    def __init__(self,
                 archive_input_file_path,
                 output_file_path,
                 archive_name_url_list):

        self.archive_input_file_path = archive_input_file_path
        self.output_file_path = output_file_path
        self.archive_name_url_list = archive_name_url_list

    def get_get_archive_urls_enabled(self):
        return self.get_archive_urls_enabled

    def set_get_archive_urls_enabled(self, bool_var):
        self.get_archive_urls_enabled = bool_var

    def get_output_file_path(self):
        return self.output_file_path

            #      """
            # Convert to ASCII if 'allow_unicode' is False. Convert spaces to hyphens.
            # Remove characters that aren't alphanumerics, underscores, or hyphens.
            # Convert to lowercase. Also strip leading and trailing whitespace.
            # """

    def slugify(value, allow_unicode=False):
        value = str(value)
        if allow_unicode:
            value = unicodedata.normalize('NFKC', value)
        else:
            value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
        value = re.sub(r'[^\w\s-]', '', value).strip().lower()
        return re.sub(r'[-\s]+', '-', value)


        # method to be called, to crawl the memento urls. Requires input_file of
        # following format (output-format should be the same!)
        # companyname;city;postcode;streetnobuildingetcline1;websiteaddress;bvdidnumber;country

    def crawl_archive_urls(self):
        # create directory to store all produced files in
        filename = self.archive_name_url_list

        if not os.path.isdir(self.output_file_path):
            os.makedirs(self.output_file_path)

        companies_not_in_archive = {}
        companies_not_in_archive['companies'] = []

        # write urls to an output .csv file
        # companyname;city;postcode;streetnobuildingetcline1;websiteaddress;bvdidnumber;country;timegate_url;datetime

        with open(os.path.join(self.output_file_path, filename), 'a') as outfile:
            writer = csv.writer(outfile, delimiter =';', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(["companyname", "city", "postcode", "streetnobuildingetcline1", "archive_url", "bvdidnumber", "country", "datetime"])


        with open(self.archive_input_file_path) as cwl:
            csvReader = csv.reader(cwl, delimiter=';')
                # skip header line
            next(csvReader)

            for row in csvReader:
                # loop through company_urls & get mementos-urls
                base_url = 'http://labs.mementoweb.org/timemap/json/http://'
                u = base_url + row[4]
                r = requests.get(u)

                # test whether request with desired company-url got valid response
                # status_code: 200 --> valid response
                if r.status_code == 200:
                    data = json.loads(r.text)
                    edited_row = row
                    edited_row.append("x")

                    # normal case: url directly accessible
                    if 'timemap_index' not in data:
                        for t in data['mementos']['list']:
                            with open(os.path.join(self.output_file_path, filename), 'a') as outfile:
                                writer = csv.writer(outfile, delimiter =';', quotechar='"', quoting=csv.QUOTE_ALL)
                                edited_row[4] = t['uri'].replace(':80/','/')
                                edited_row.append(t['datetime'])
                                writer.writerow(edited_row)
                            outfile.close()

                    # badge case --> first open url of badge
                    else:
                        for b in data['timemap_index']:
                            badge = b['uri']
                            r = requests.get(badge)
                            badge_data = json.loads(r.text)

                            for t in badge_data['mementos']['list']:
                                with open(os.path.join(self.output_file_path, filename), 'a') as outfile:
                                    writer = csv.writer(outfile, delimiter =';', quotechar='"', quoting=csv.QUOTE_ALL)
                                    edited_row[4] = t['uri'].replace(':80/','/')
                                    edited_row[7] = t['datetime']
                                    writer.writerow(edited_row)
                                outfile.close()

                    # no valid response (status code != 200)
                    # json to store all companies, where no valid archive url response was obtained
                else:
                    with open(os.path.join(self.output_file_path, "companies-not-in-archive.csv"), 'a') as outfile:
                        writer = csv.writer(outfile, delimiter =';', quotechar='"', quoting=csv.QUOTE_ALL)
                        writer.writerow(["companyname", "city", "postcode", "streetnobuildingetcline1", "url", "bvdidnumber", "country", "status-code"])
                        edited_row.append(r.status_code)
                        writer.writerow(edited_row)
                    outfile.close()
