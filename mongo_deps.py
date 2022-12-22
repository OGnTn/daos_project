import pymongo
import numpy

mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
dblp = mongoclient["testdb"]
articles_collection = dblp["articles"]
proceeding_collection = dblp["proceedings"]
inproceeding_collection = dblp["inproceedings"]
proceeding_collection.create_index([("editor", 1)])
inproceeding_collection.create_index([("author", 1)])


class Query:
    def __init__(self, q, col):
        self.q = q
        self.col = col

    def run_query(self):
        return self.col.aggregate(self.q)


e1 = Query([
    {
        "$match": {
            "booktitle": "PODS"
        }
    },
    {
        "$project": {
            "_id": 0,
            "publisher": "$publisher"
        }
    },
    {
        "$limit": 1
    }
], proceeding_collection)

e2 = Query([
    {
        "$match": {
            "author": "Martin Grohe"
        }
    },
    {
        "$match": {
            "journal": "Theory Comput. Syst."
        }
    },
    {
        "$project": {
            "_id": 0,
            "title": "$title"
        }
    }
], articles_collection)

m1 = Query([
    {
        "$match":
        {
            "$expr":
            {
                "$regexMatch": {
                    "input": "$booktitle",
                    "regex": "SIGMOD"
                }
            }
        }
    },
    {
        "$match": {
            "year": "2022"
        }
    },
    {
        "$count": "article_count"
    }
], inproceeding_collection)

m2 = Query([
    {
        "$set":
        {
            "year":
            {
                "$cond": [{"$eq": ['$year', numpy.NaN]}, "MAXVAL", '$year']
            }
        }
    },
    {
        "$group":
        {
            "_id": "$journal",
            "count": {"$sum": 1},
            "year": {"$min": "$year"}
        }
    },
    {
        "$sort": {"year": 1}
    },
    {
        "$limit": 1
    }
], articles_collection)

m3 = Query([
    {
        "$match": {
            "booktitle": "CIDR"
        }
    },
    {
        "$group": {
            "_id": "$year",
            "num_records": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "num_records": 1
        }
    },
    {
        "$group":
        {
            "_id": "$booktitle",
            "valueArray":
            {
                "$push": "$num_records"
            }
        }
    },
    {
        "$project": {
            "_id": 1,
            "valueArray": 1,
            "size": {"$size": ["$valueArray"]}
        }
    },
    {
        "$project": {
            "_id": 1,
            "valueArray": 1,
            "mid": {"$trunc": {"$divide": ["$size", 2]}}
        }
    },
    {
        "$project": {
            "_id": 0,
            "median": {
                "$arrayElemAt": ["$valueArray", "$mid"]
            }
        }
    }
], inproceeding_collection)

m4 = Query([
    {
        "$match": {
            "$expr":
            {
                "$regexMatch": {
                    "input": "$booktitle",
                    "regex": "SIGMOD"
                }
            }
        }
    },
    {
        "$match": {
            "author.10": {"$exists": "true"}
        }
    },

    {
        "$group": {
            "_id": "$year",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "count": -1
        }
    },
    {
        "$limit": 1
    }
], inproceeding_collection)

m5 = Query([
    {
        "$match":
        {
            "booktitle": {"$ne": numpy.NaN}
        }
    },
    {
        "$match": {
            "$expr":
            {
                "$regexMatch": {
                    "input": "$booktitle",
                    "regex": "PODS",
                    "options": "i"
                }
            }
        }
    },
    {
        "$unwind": "$editor"
    },
    {
        "$group": {
            "_id": "$editor",
            "count": {"$sum": 1},
        }
    },
    {
        "$sort": {
            "count": -1
        }
    },
    {
        "$group": {
            "_id": "$count",
            "names": {
                "$push": "$_id"
            }
        }
    },
    {
        "$sort": {
            "_id": -1
        }
    },
    {
        "$limit": 1
    },
    {
        "$project": {
            "names": 1,
            "amount": "$_id",
            "_id": 0
        }
    }

], proceeding_collection)

m6 = Query([
    {
        "$unwind": "$editor"
    },
    {
        "$match":
        {
            "editor": {"$ne": numpy.NaN}
        }
    },
    {
        "$group": {
            "_id": "$editor",
            "booktitles": {
                "$push": "$booktitle"
            },
            "editorCount": {
                "$sum": 1
            }
        }
    },
    {
        "$lookup": {
            "from": "inproceedings",
            "localField": "_id",
            "foreignField": "author",
            "as": "inproceedings"
        }
    },
    {
        "$unwind": "$inproceedings"
    },
    {
        "$group": {
            "_id": "$_id",
            "booktitles": {
                "$push": "$inproceedings.booktitle"
            }
        }
    },
    {
        "$project": {
            "_id": 1,
            "booktitles": 1,
            "publication_count":
            {
                "$size": "$booktitles"
            }
        }
    },
    {
        "$sort": {
            "publication_count": -1
        }
    },
    {
        "$limit": 1
    },
    {
        "$unwind": "$booktitles"
    },
    {
        "$group": {
            "_id": "$booktitles",
            "count": {"$sum": 1}
        }
    },
    {
        "$count": "distinct_booktitles"
    }

], proceeding_collection)

mongo_queries = [e1, e2, m1, m2, m3, m4, m5, m6]
