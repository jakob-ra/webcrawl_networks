import re
import csv
import datetime as dt
import os
import json
import pandas as pd
import numpy as np


"""
aggregate individual crawled text into a single file
"""
def aggData(errors):
    ##take most current file
    d = os.listdir("../data")
    d.sort()
    ##list folders
    folders = os.listdir("../data/"+d[-1])
    c = 0
    os.system("cat ../data/"+d[-1]+"/"+folders[0]+"/text.csv > f_"+str(c)+".tmp")
    folders.pop(0)
    dirlist = "cat f_"+str(c)+".tmp "
    for fo in folders:
        c += 1
        ##skip errorenous files
        if fo in errors:
            continue
        os.system("tail -n +2 ../data/"+d[-1]+"/"+fo+"/text.csv > f_"+str(c)+".tmp")
        dirlist += "f_"+str(c)+".tmp "
    dirlist += "> temp.csv"
    os.system(dirlist)
    os.system("rm *.tmp")


"""
index aggregate file by domain and url
to-do: take hierarchy into account 
"""
def indexData():
    data = pd.read_csv("temp.csv", sep=";", quotechar="'", quoting=csv.QUOTE_ALL)
    data = data.assign(bvdidID=(data["bvdid"]).astype("category").cat.codes)
    ##create urlID within each bvdid already with scrapy script 
    data["urlID"] = data.groupby("bvdidID")["url"].transform(lambda x: pd.factorize(x)[0])
    data.to_csv("data.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
    os.remove("temp.csv")


"""
high-level checks whether data download is complete
and whether for each domain depth has been reached!
"""
def dataComplete():
    ##check for which companies there is no data
    errs = ["HttpError", "DNSLookupError", "TimeoutError", "NoEnglishLang"]
    urls = pd.read_csv("../dataMichael/Orbis/orbis_webaddresses/us/Orbis_Web_US_uniqueURLs.csv",sep=";")
    files = os.listdir("../data")
    files.sort()
    errors = []
    print("====== Websites with errors:")
    for f in os.listdir("../data/"+files[-1]):
        temp = pd.read_csv("../data/"+files[-1]+"/"+f+"/text.csv",sep=";",quotechar="'")
        if temp.shape[0]==1:
            errors.append(f)
            print("\t%s (%s) with following error: %s" % (temp["bvdid"].iloc[0],temp["url"].iloc[0],temp["text"].iloc[0]))
            continue
        elif temp.shape[0]==0:
            errors.append(f)
            print("\t%s (%s) with following error: zero rows" % (f,urls[urls["bvdidnumber"]==f]["websiteaddress"].iloc[0]))
            continue
        ##number of links as further check
        nlinks = len(temp["url"].unique())
        links = pd.read_csv("../data/"+files[-1]+"/"+f+"/links.csv",sep=";",quotechar="'")
        links = links[links["origin"]==links["origin"].iloc[0]]
        if np.abs(nlinks-links.shape[0])<0.95*links.shape[0]:
            print("\t%s (%s) with links whose content was not retrieved: %i (is) vs. %i (should)" % (f,urls[urls["bvdidnumber"]==f]["websiteaddress"].iloc[0],nlinks,links.shape[0]))
    print("=====")
    return errors


if __name__=="__main__":
    errors = dataComplete()
    aggData(errors)
    indexData()
