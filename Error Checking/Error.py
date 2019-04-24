# Import Libraries

import pymongo
from pymongo import MongoClient
import pprint
import re
from bs4 import BeautifulSoup
import urllib
import httplib2
import json



# coonecting with mongo db

def connect_mongodb(config):

    client = MongoClient(config["database"]://config["username"]:config["password"]@config["mongourl"])

    return client

# choose collection and field
def db(config,client):
    db = client[config["data"]]
    db = db[config["collection"]]
    return db

pr1 = db.find({}).count()
pr2 = db.find({"Price":{"$gte": 0}}).count()

#updating price field

def update_price(config,db):

    db.update_many({"Price":"None"}, {"$set": {config["errorprice"]: True}})



    while True:
        cursor = db.find({config["errorprice"]: True},no_cursor_timeout=True)

        print(cursor.count())
        for doc in cursor:
            if ("urltoproduct" in list(doc.keys())):
                url = doc['urltoproduct']
                try:
                    fp = urllib.request.urlopen(httplib2.iri2uri("https:"+urllib.parse.quote(url).split("%3A")[1]))
                    htmltext = fp.read()
                    soup = BeautifulSoup(htmltext)
                except Exception as e:
                    print(str(e))
                try:
                    soup.find('ul', id="menu").decompose()
                    prc = float(re.findall(r'\d+\,?\d{0,2}[€]',soup.text)[0].replace(",",".").replace("€",""))
                    db.update({"_id":doc['_id']}, {"$set": {"Price":prc}})
                    db.update({"_id": doc['_id']}, {"$unset": {"errorprice": ""}})
                except:
                    continue





        break

def urlproduct(config,db):

    ##urlproduct =  db.find({"urltoproduct": True},no_cursor_timeout=True)

    urlproduct = db.find({"urltoproduct" : {"$regex" : "^(?!http.)"},"urltoproduct" : {"$regex" : "^(?!www.)"},"urltoproduct" : {"$regex" : "^(?!https.)"}})


    for doc in urlproduct:
        if (doc['urltoproduct'] != "None"):
            url = "https://"+doc['Source'].split("/")[2]+doc['urltoproduct']
            db.update({"_id": doc['_id']}, {"$set": {"urltoproduct": url}})
        else:
            db.update({"_id": doc['_id']}, {"$set": {config["errorurltoproduct"]: True}})
    return


# image error
def image_error(config,db)
    query = {}
    query["$and"] = [
        {
            u"Imagefilename": {
                u"$not": Regex(u".*png.*", "i")
            }
        },
        {
            u"Imagefilename": {
                u"$not": Regex(u".*jpg.*", "i")
            }
        },
        {
            u"Imagefilename": {
                u"$not": Regex(u".*jpeg.*", "i")
            }
        }
    ]


    cursor = db.find(query,no_cursor_timeout = True)

    for doc in cursor:
        db.update({"_id": doc['_id']}, {"$set": {config["image_error"]: True}})
    return

def main(config):
    client = connect_mongodb(config)
    db = db(config,client)
    update_price= update_price(config,db)
    urlproduct= urlproduct(config,db)
    image_error = image_error(config,db)
    pass


if __name__ == "__main__":
    with open('/home/arajan/Documents/Githubrepo/PharmaScraper/Error Checking/config.json') as jsonfile:
        config = json.load(jsonfile)
    main(config)