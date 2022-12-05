from logging import root
from pprint import pprint
from lxml import etree
from io import BytesIO
import pandas as pd
from xlwings import view
import numpy as np
import pymongo
import re

mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
testdb = mongoclient["testdb"]
testarticlescol = testdb["articles"]
testproceedingcol = testdb["proceedings"]
testinproceedingcol = testdb["inproceedings"]

article_attributes = ['key', 'author', 'title',
                      'journal', 'volume', 'number', 'year']
proceeding_attributes = ['key', 'editor', 'title', 'booktitle',
                         'publisher', 'volume', 'year']
inproceeding_attributes = ['key', 'author',
                           'title', 'pages', 'year', 'booktitle']


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
            element.clear()
        elif element.tag == "proceedings":

            current_proceeding = {}
            current_proceeding["key"] = element.attrib.get('key')
            for attribute in list(filter(lambda x: x != 'key', proceeding_attributes)):
                current_proceeding[attribute] = mymap(
                    lambda x: x.text, element.findall(attribute))
            proceedings.append(current_proceeding)
            element.clear()
        elif element.tag == "inproceedings":

            current_inproceeding = {}
            current_inproceeding["key"] = element.attrib.get('key')
            for attribute in list(filter(lambda x: x != 'key', inproceeding_attributes)):
                current_inproceeding[attribute] = mymap(
                    lambda x: x.text, element.findall(attribute))
            inproceedings.append(current_inproceeding)
            element.clear()
    print("entries parsed.")
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
