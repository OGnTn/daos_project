from logging import root
from pprint import pprint
from lxml import etree
from xlwings import view
import numpy as np
import pymongo
# Parser script to parse the dblp xml and insert into a mongoDB database.

# Custom map that returns a nan if an empty list is given,
# the only element if a list of size one is given or just the list.
# It applies fun to every element.


def nanmap(fun, list):
    if list == []:
        return np.nan
    elif len(list) == 1:
        return fun(list[0])
    else:
        l = []
        for e in list:
            l.append(fun(e))
        return l

# Parsing function that takes the xml to parse


def parse(xml_file):

    # Mongo setup
    mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
    testdb = mongoclient["testdb2"]
    testarticlescol = testdb["articles"]
    testproceedingcol = testdb["proceedings"]
    testinproceedingcol = testdb["inproceedings"]

    # The attributes that each type of publication has
    article_attributes = ['key', 'author', 'title',
                          'journal', 'volume', 'number', 'year']
    proceeding_attributes = ['key', 'editor', 'title', 'booktitle',
                             'publisher', 'volume', 'year']
    inproceeding_attributes = ['key', 'author',
                               'title', 'pages', 'year', 'booktitle']

    # A dictionary to match on the tag of a publication and get the correct attributes.
    attr_dict = {
        "article": article_attributes,
        "inproceedings": inproceeding_attributes,
        "proceedings": proceeding_attributes
    }

    # the collections that will be inserted into the database using insert_many.
    articles = []
    inproceedings = []
    proceedings = []

    # A dictionary to match on the tag of a publication and get the correct collections.
    data_dict = {
        "article": articles,
        "inproceedings": inproceedings,
        "proceedings": proceedings
    }

    # Parse each element that has either of the three as tag. The correct attributes are collected and the elements are constructed as dictionaries.
    for _, element in etree.iterparse(source=xml_file, dtd_validation=True, load_dtd=True):
        tag = element.tag
        if tag == "article" or tag == "inproceedings" or tag == "proceedings":
            current_element = {}
            current_element["key"] = element.attrib.get('key')
            for attribute in list(filter(lambda x: x != 'key', attr_dict[tag])):
                current_element[attribute] = nanmap(
                    lambda x: x.text, element.findall(attribute))
            data_dict[tag].append(current_element)
            element.clear()

    # insert the parsed elements into the respective collection.
    print("entries parsed.")
    print("inserting articles.")
    testarticlescol.insert_many(data_dict['article'])
    print("articles inserted.")
    print("inserting inproceedings.")
    testinproceedingcol.insert_many(data_dict['inproceedings'])
    print("inproceedings inserted.")
    print("inserting proceedings.")
    testproceedingcol.insert_many(data_dict['proceedings'])
    print("inproceedings inserted.")
    print("file parsed")


parse('dblp.xml')
