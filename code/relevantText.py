import pandas as pd
import csv
import sys
import re


"""
heuristic: remove textual content that appears at least on x% of subpages of a domain
and delete duplicate text information
-> alternatively: delete non-unique text information!
TO-DO: consider tag path as well (?)
"""
def withinRelev(mode):
    data = pd.read_csv("data.csv", sep=";")
    temp = data.groupby(["bvdidID", "text"]).nunique("urlID")
    temp["bvdidID"] = [i[0] for i in temp.index]
    temp["text"] = [i[1] for i in temp.index]
    temp = temp.reset_index(drop=True)
    if mode=="all":
        ##only keep unique text information
        temp = temp[temp["urlID"]==1]
        data = pd.merge(data, temp, on=["bvdidID", "text"], how="inner")
    else:
        ##remove text that appears on x% of the subpages
        countUrl = data.groupby(["bvdidID"]).nunique("urlID")
        countUrl["bvdidID"] = countUrl.index
        combined = pd.merge(temp[["bvdidID","text","urlID"]],countUrl[["bvdidID","urlID"]],"left",on="bvdidID")
        combined = combined[combined["urlID_x"]<0.8*combined["urlID_y"]]
        ##index text variable -> check for large data amounts
        data = pd.merge(data, combined[["bvdidID", "text"]], ["bvdidID", "text"], how="inner")
    ##remove duplicate text
    data = data.drop_duplicates(["bvdidID", "text"])
    data.to_csv("data1.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)


"""
prepare labelled data for between relevance classification
assumptions: rel.: first page of domain and heuristic based; irrel.: heuristic based
"""
def prepData4BR():
    data = pd.read_csv("data_embedded.csv", sep=";")
    data["text2id"] = data["text2id"].apply(lambda x: list(map(int, re.findall("\d+", x))))
    ##to-do: used indexing when speed issues
    urls = data["url_x"].unique()
    ##main page
    subdata = data[data["urlID_x"]==0]
    relev = pd.DataFrame(subdata.groupby("bvdidID")["text2id"].apply(lambda x: x.sum()))
    relev.reset_index(inplace=True)
    relev["urlID_x"] = 0
    ##list of keywords in urls that very likely define a relevant webpage
    identRelev = "(about|product|service)"
    keepRelev = [i for i in urls if re.search(identRelev,i) is not None]
    relev2 = data[data["url_x"].isin(keepRelev)]
    relev2 = pd.DataFrame(relev2.groupby(["bvdidID", "urlID_x"])["text2id"].apply(lambda x: x.sum()))
    relev2.reset_index(inplace=True)
    relev = relev.append(relev2, sort=True)
    relev["relevant"] = 1
    ##list of keywords in urls that very likely define an irrelevant webpage
    identIrrelev = "(termsofuse|privacy|data|contact|impressum|search|disclaimer|cookie|investors|site-terms|faqs|compliance|login|support)"
    keepIrrelev = [i for i in urls if re.search(identIrrelev,i) is not None]
    irrelev = data[data["url_x"].isin(keepIrrelev)]
    irrelev = pd.DataFrame(data.groupby(["bvdidID", "urlID_x"])["text2id"].apply(lambda x: x.sum()))
    irrelev.reset_index(inplace=True)
    irrelev["relevant"] = 0
    output = pd.concat([relev, irrelev], axis=0, ignore_index=True, sort=True)
    ##to-do: check for ambigious cases
    output = output.drop_duplicates(["bvdidID", "urlID_x"])
    output.to_csv("dataBetweenRelevance.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
    

"""
remove complete subdomains related to AGBs, data privacy, and similar
"""
def betweenRelev():
    prepData4BR()
    ##estimate classification model in tf using word embeddings
    ...


if __name__=="__main__":
    if sys.argv[1]=="within":
        withinRelev(mode="all")
    elif sys.argv[1]=="between":
        prepData4BR()
        ##betweenRelev()
    else:
        raise ValueError("Please provide a valid argument.")
