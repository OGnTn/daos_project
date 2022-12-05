from logging import root
from pprint import pprint
from lxml import etree
from io import BytesIO
import pandas as pd
from xlwings import view
import numpy as np
import pymongo
import re

XML_FILE = "miauw.xml"

mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
testdb = mongoclient["testdb"]
testarticlescol = testdb["articles"]
testproceedingcol = testdb["proceedings"]
testinproceedingcol = testdb["inproceedings"]


def get_all(element, tag):
    elements = np.array(list(map(lambda x: x.text, element.findall(tag))))
    if len(elements) > 1:
        values = " --- ".join(elements)
        return values
    elif len(elements) == 1:
        return elements[0]
    else:
        return np.nan


# attribute to parse article = ['key','title','journal','volume','number','year']
# attribute to parse proceedings = ['key','editor','title','publisher','year','booktitle','volume']
# attribute to parse inproceedings = ['key','author','title','pages','year','booktitle']
article_attributes = ['key', 'author', 'title',
                      'journal', 'volume', 'number', 'year']
proceeding_attributes = ['key', 'editor', 'title', 'booktitle',
                         'publisher', 'volume', 'year']
inproceeding_attributes = ['key', 'author',
                           'title', 'pages', 'year', 'booktitle']

miauw = [{"test": "woef"}]


def map_or_value(map):
    if(len(list(map)) > 1):
        # print(list(map))
        return list(map)
    elif (len(list(map)) == 1):
        return list(map)[0]
    else:
        return np.nan


def mymap(fun, list):
    if list == []:
        return np.nan
    elif len(list) == 1:
        return fun(list[0])
    else:
        l = []
        for e in list:
            l.append(fun(e))
        return l


def parse(xml_file):
    # make sure to include key in attributes
    article_data = dict(
        zip(article_attributes, [[] for attrib in range(len(article_attributes))]))
    proceeding_data = dict(
        zip(proceeding_attributes, [[] for attrib in range(len(proceeding_attributes))]))
    inproceeding_data = dict(
        zip(inproceeding_attributes, [[] for attrib in range(len(inproceeding_attributes))]))

    articles = []
    inproceedings = []
    proceedings = []
    for _, element in etree.iterparse(source=xml_file, dtd_validation=True, load_dtd=True):
        if element.tag == "article":
            current_article = {}
            current_article["key"] = element.attrib.get('key')
            for attribute in list(filter(lambda x: x != 'key', article_attributes)):
                current_article[attribute] = mymap(
                    lambda x: x.text, element.findall(attribute))
            articles.append(current_article)
            # adding them to the database(this case dict in order to be transformed into pandas dataframe and then csv file)
            # article_data["key"].append(element.attrib.get('key'))
            # for attribute in list(filter(lambda x: x != 'key', article_attributes)):
            #    article_data[attribute].append(get_all(element, attribute))
            element.clear()
        elif element.tag == "proceedings":

            current_proceeding = {}
            current_proceeding["key"] = element.attrib.get('key')
            for attribute in list(filter(lambda x: x != 'key', proceeding_attributes)):
                current_proceeding[attribute] = mymap(
                    lambda x: x.text, element.findall(attribute))
            proceedings.append(current_proceeding)

            # adding them to the database(this case dict in order to be transformed into pandas dataframe and then csv file)
            # proceeding_data["key"].append(element.attrib.get('key'))
            # for attribute in list(filter(lambda x: x != 'key', proceeding_attributes)):
            #    proceeding_data[attribute].append(get_all(element, attribute))
            element.clear()
        elif element.tag == "inproceedings":

            current_inproceeding = {}
            current_inproceeding["key"] = element.attrib.get('key')
            for attribute in list(filter(lambda x: x != 'key', inproceeding_attributes)):
                current_inproceeding[attribute] = mymap(
                    lambda x: x.text, element.findall(attribute))
            inproceedings.append(current_inproceeding)

            # adding them to the database(this case dict in order to be transformed into pandas dataframe and then csv file)
            # inproceeding_data["key"].append(element.attrib.get('key'))
            # for attribute in list(filter(lambda x: x != 'key', inproceeding_attributes)):
            #    inproceeding_data[attribute].append(
            #        get_all(element, attribute))
            element.clear()
    print("entries parsed.")
    #article_df = pd.DataFrame(article_data)
    # article_df.to_csv("article.csv")
    #proceeding_df = pd.DataFrame(proceeding_data)
    # proceeding_df.to_csv("proceeding.csv")
    #inproceeding_df = pd.DataFrame(inproceeding_data)
    # inproceeding_df.to_csv("inproceeding.csv")
    print("inserting articles.")
    testarticlescol.insert_many(articles)
    print("articles inserted.")
    print("inserting inproceedings.")
    testinproceedingcol.insert_many(inproceedings)
    print("inproceedings inserted.")
    print("inserting proceedings.")
    testproceedingcol.insert_many(proceedings)
    print("inproceedings inserted.")

    print("file parsed")


parse('dblp.xml')
