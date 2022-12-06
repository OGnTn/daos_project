from logging import root
from pprint import pprint
from lxml import etree
from io import BytesIO
import pandas as pd 
from xlwings import view
import numpy as np
import pickle
import bz2
import hickle as hkl


import gzip, pickle, pickletools



XML_FILE= "dblp.xml"

def get_all(element,tag):
    elements =  np.array(list(map(lambda x : x.text,element.findall(tag))))
    if len(elements) >1 :
        values = " ---" .join(elements)
        return values
    elif len(elements)==1:
        return elements[0]
    else:
        return np.nan


# attribute to parse article = ['key','title','journal','volume','number','year']
ARTICLE_ATTRIBUTES = ['key','author','title','journal','volume','number','year']

# attribute to parse proceedings = ['key','editor','title','publisher','year','booktitle','volume']
PROCEEDING_ATTRIBUTES = ['key','editor','title','publisher','year','booktitle','volume']

# attribute to parse inproceedings = ['key','author','title','pages','year','booktitle']
INPROCEEDING_ATTRIBUTES = ['key','author','title','pages','year','booktitle']

def parse(toParse,attributes,xml_file,csv_File):
    ## make sure to include key in attributes
    print("Parsing started")
    data = dict(zip(attributes,[[] for attrib in range(len(attributes))]))
    for _, element in etree.iterparse(source=XML_FILE, tag=toParse,dtd_validation=True, load_dtd=True):
        #adding them to the database(this case dict in order to be transformed into pandas dataframe and then csv file)
        data["key"].append(element.attrib.get('key'))
        for attribute in list(filter(lambda x: x!= 'key',attributes)) :
            data[attribute].append(get_all(element,attribute))
        element.clear()
    
    df = pd.DataFrame(data)
    df.to_feather(f"./feathers/{toParse}.feather")  
    print(f"{toParse} parsed")


parse('article',ARTICLE_ATTRIBUTES,XML_FILE,'test_parse.csv')

