import csv
import pandas as pd
from gensim.models import KeyedVectors
import os


"""
create vocabulary and ids
"""
def createDict(model):
    vocab = list(model.wv.vocab.keys())
    iddict = dict()
    for i in range(len(vocab)): iddict[vocab[i]] = i
    out = pd.DataFrame({"id": [i for i in iddict.keys()], "word": [i for i in iddict.values()]})
    out.to_csv("word2id.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
    return(iddict)


"""
helper function to transform text into sequence of ints
to-do: find gensim function that does the same but faster
""" 
def word2id(text, model, f):
    text = text.split(" ")
    text = [i for i in text if len(i)>0]
    out = []
    for i in text:
        ##index starts with 0
        try:
            out.append(model.vocab[i].index)
        except KeyError:
            f.write(i+"\n")
    return(out)

               
"""
transform text data to int ids
"""
def text2id(model):
    data = pd.read_csv("data1.csv", sep=";")
    ##write KeyErrors to file
    f = open("KeyErrors.csv", "w")
    data["text2id"] = data["text"].apply(lambda x: word2id(x, model, f))
    f.close()
    data.to_csv("data_embedded.csv", sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)


if __name__=="__main__":
    ##word vectors: https://fasttext.cc/docs/en/english-vectors.html
    embedEN = KeyedVectors.load_word2vec_format("pretrainedModels/crawl-300d-2M.vec")
    if "word2id.csv" not in os.listdir(os.getcwd()):
        createDict(embedEN)
    text2id(embedEN)
    
