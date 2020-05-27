import pandas as pd
import langid as li
import re
import fastText
import csv


"""
uncapitalize, replace vowels, tokenize sentences, only keep alpha characters, delete empty text entries
"""
def cleanData():
    data = pd.read_csv("data.csv", sep=";")
    data["text"] = data["text"].apply(str)
    data["text"] = data["text"].apply(lambda x: x.lower())
    # data["text"] = data["text"].apply(lambda x: re.sub("(\xc3\x84|ä)", "ae", x))
    # data["text"] = data["text"].apply(lambda x: re.sub("(\xc3\x9c|ü)", "ue", x))
    # data["text"] = data["text"].apply(lambda x: re.sub("(\xc3\xb6|ö)", "oe", x))
    ##see for tokenization rules: https://github.com/facebookresearch/fastText/blob/master/python/README.md
    tokenizer = lambda x: " ".join(fastText.tokenize(x))
    data["text"] = data["text"].apply(tokenizer)
    ##delete any non-alpha character
    data["text"] = data["text"].apply(lambda x: re.sub("[^A-Za-z ]", "", x))
    ##delete if text empty
    data = data[data["text"]!=""]
    data.to_csv("data.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)


"""
detect language
"""
def detectLang():
    data = pd.read_csv("data.csv", sep=";")
    ##data["lang"] = data["text"].apply(lambda x: li.classify(x)[0])
    ##fasttext is faster with comparable performance, see https://fasttext.cc/docs/en/language-identification.html
    lang = fastText.load_model("pretrainedModels/lid.176.bin")
    data["text"] = data["text"].apply(str)
    data["lang"] = data["text"].apply(lambda x: lang.predict(x)[0][0])
    ##only keep if lang is en
    keep = ["__label__en"]##+["__label__de", "__label__it", "__label__fr"]
    data = data[data["lang"].isin(keep)]
    data.to_csv("data.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
 

if __name__=="__main__":
    cleanData()
    detectLang()
