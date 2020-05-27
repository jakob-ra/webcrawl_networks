# -*- coding: utf-8 -*-
import sys
import psutil
import json
import os
import time
import csv
import math
import subprocess
import re
import shlex
import multiprocessing as mp
from util import split_csv

# Get system information
SYSTEM_CPU_CORES = psutil.cpu_count()   # Works platform independent, compared to e.g. multiprocessing.cpu_count()
DEDICATED_CORES = int(SYSTEM_CPU_CORES * 0.65) - 1  # One process is this main process, therefore -1

# Instantiate variables
URL_COUNT = 0

# Read configurations from file
with open('settings.json') as f:
    SETTINGS_FILE = json.load(f)
    f.close()

# Set configurations from file
PROJECT_NAME = SETTINGS_FILE.get('projectName') 
PATH_URL_LIST = SETTINGS_FILE.get('pathToCSVUrlList')
NAME_URL_LIST = SETTINGS_FILE.get('nameUrlList')
CSV_DELIMITER = SETTINGS_FILE.get("csvDelimiter")
PAGES_PER_DOMAIN = int(SETTINGS_FILE.get("pagesPerDomain"))
DEPTH_PER_DOMAIN = int(SETTINGS_FILE.get("depthPerDomain"))                                             
LANGUAGE_PREFERENCE = SETTINGS_FILE.get("languagePreference")
OUTPUT_PATH = SETTINGS_FILE.get("outputFilePath")
GET_ARCHIVE_URLS_ENABLED = SETTINGS_FILE.get("GetArchiveUrls")
ARCHIVE_INPUT_FILE_PATH = SETTINGS_FILE.get("ArchiveInputFilePath")
ARCHIVE_OUTPUT_FILEPATH = SETTINGS_FILE.get("ArchiveOutputFilePath")
ARCHIVE_NAME_URL_LIST = SETTINGS_FILE.get("nameArchiveList")

# Read number of rows from csv file
with open(PATH_URL_LIST + NAME_URL_LIST, encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f, delimiter=CSV_DELIMITER, skipinitialspace=True)

    next(reader)  # skip header
    URL_COUNT = sum(1 for row in reader)
    f.close()
    
# Terminate Program if no rows found in CSV
if URL_COUNT < 1:
    print("No url's found in url-list.csv (first row was removed as header).")
    sys.exit()
    
# Calculate number of processes
NUMBER_OF_PROCESSES = SETTINGS_FILE.get('numberOfProcesses')
MIN_URL_PER_PROCESS = 10
if NUMBER_OF_PROCESSES > SYSTEM_CPU_CORES-1:
    NUMBER_OF_PROCESSES = SYSTEM_CPU_CORES-1
NUMBER_OF_PROCESSES = min(math.ceil((URL_COUNT / MIN_URL_PER_PROCESS)), NUMBER_OF_PROCESSES)

# Set name and create directory
DIRECTORY_NAME = re.sub(r'[/\\:*?"<>| ]', '-', PROJECT_NAME) + "-" + time.strftime("%Y%m%d–%H%M%S") + "/"
OUTPUT_PATH = OUTPUT_PATH + DIRECTORY_NAME
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# Split url list into multiple csv files
FILE_PREFIX = re.sub(".csv", "", NAME_URL_LIST)
RECORDS_PER_FILE = URL_COUNT / NUMBER_OF_PROCESSES
"""
for testing purposes only (delete later)
"""
RECORDS_PER_FILE = 1000
if NUMBER_OF_PROCESSES > 1:
    split_csv(PATH_URL_LIST + NAME_URL_LIST, PATH_URL_LIST, FILE_PREFIX, RECORDS_PER_FILE, CSV_DELIMITER)
else:
    os.rename((PATH_URL_LIST + NAME_URL_LIST), (PATH_URL_LIST + NAME_URL_LIST.replace(".csv", "") + "_0.csv"))


# Run processes with configuration and parameter to distribute data
##, PATH_URL_LIST, NAME_URL_LIST, OUTPUT_PATH, PAGES_PER_DOMAIN, LANGUAGE_PREFERENCE, DEPTH_PER_DOMAIN
def createCommand(n):
    sub_process_command = "python3 processcontroller.py " \
                          + (PATH_URL_LIST + NAME_URL_LIST.replace(".csv", "") + "_" + str(n) + ".csv") + " " \
                          + OUTPUT_PATH + " "\
                          + str(PAGES_PER_DOMAIN) + " " \
                          + LANGUAGE_PREFERENCE + " " \
                          + str(DEPTH_PER_DOMAIN) + " " \
                          + str(GET_ARCHIVE_URLS_ENABLED) + " " \
                          + str(ARCHIVE_INPUT_FILE_PATH) + " " \
                          + str(ARCHIVE_OUTPUT_FILEPATH) + " " \
                          + str(ARCHIVE_NAME_URL_LIST)

#                          + str(s) +" "+ str(e)
    os.system(sub_process_command)
    
# def helper(n):
#     s = 0
#     e = MAX_BATCH_SIZE
#     while e<RECORDS_PER_FILE:
#         s += MAX_BATCH_SIZE
#         e = min(e+MAX_BATCH_SIZE, RECORDS_PER_FILE)
#         createCommand(n,s,e)

# createCommand(1,1,1)
# MAX_BATCH_SIZE = 10
p = mp.Pool(NUMBER_OF_PROCESSES)
p.map(createCommand, [0])#range(NUMBER_OF_PROCESSES))
