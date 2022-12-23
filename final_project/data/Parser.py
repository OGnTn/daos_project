# Import necessary libraries
from lxml import etree
import pandas as pd 
import numpy as np
import os


# Path to the XML file to be parsed
XML_FILE= "dblp.xml"

# Function to extract text from all child elements of a specified tag and return the values as a string
# If there are multiple child elements, the values are joined with ' ---'. 
# If there is only one child element, the value is returned as is. 
# If there are no child elements, 'np.nan' is returned.
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


# Function to parse an XML file and extract data from elements with a specified tag
# The extracted data is stored in a dictionary and then converted into a Pandas DataFrame
# The DataFrame is saved as a feather file
def parse(toParse,attributes,xml_file):
      
    # create folder if not exist for the feather files
    # exist or not.
    if not os.path.exists("./feathers"):
        os.makedirs("./feathers")

    ## make sure to include key in attributes
    print("Parsing started")
    # Initialize an empty dictionary to store the extracted data
    data = dict(zip(attributes,[[] for attrib in range(len(attributes))]))
    # Iterate through the elements with the specified tag in the XML file
    for _, element in etree.iterparse(source=XML_FILE, tag=toParse,dtd_validation=True, load_dtd=True):
        # Add the element's 'key' attribute to the data dictionary
        data["key"].append(element.attrib.get('key'))

        # Iterate through the list of attributes and extract the text from the child elements with the corresponding tag
        for attribute in list(filter(lambda x: x!= 'key',attributes)) :
            data[attribute].append(get_all(element,attribute))
        # Clear the element to save memory
        element.clear()

    # Convert the data dictionary into a Pandas DataFrame
    df = pd.DataFrame(data)

    # Save the DataFrame as a feather file
    df.to_feather(f"./feathers/{toParse}.feather")  
    # Print a message indicating that parsing has completed
    print(f"{toParse} parsed")


# Call the 'parse' function to extract data from 'article' elements in the XML file
parse('article',ARTICLE_ATTRIBUTES,XML_FILE)

