import pymongo
from pymongo import MongoClient
import pprint
import re
from bs4 import BeautifulSoup
import urllib
import httplib2






client = MongoClient("mongodb://abhir:AbhiPrecision19@mongodb-712-0.cloudclusters.net:27017")

db = client['pharmascrape']
db = db['scrapes']

pr1 = db.find({}).count()
pr2 = db.find({"Price":{"$gte": 0}}).count()

db.update_many({"Price":"None"}, {"$set": {"errorprice": True}})

while True:
    cursor = db.find({"errorprice": True},no_cursor_timeout=True)

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

urlproduct =  db.find({"urltoproduct": True},no_cursor_timeout=True)




urlproduct = db.find({"urltoproduct" : {"$regex" : "^(?!http.)"},"urltoproduct" : {"$regex" : "^(?!www.)"},"urltoproduct" : {"$regex" : "^(?!https.)"}})

print(urlproduct.count())


for doc in urlproduct:
    if (doc['urltoproduct'] != "None"):
        url = "https://"+doc['Source'].split("/")[2]+doc['urltoproduct']
        db.update({"_id": doc['_id']}, {"$set": {"urltoproduct": url}})
    else:
        db.update({"_id": doc['_id']}, {"$set": {"errorurltoproduct": True}})


# image error
imgnm = db.find({"Imagefilename" : {"$regex" : ".*(?<!jpg)$"}})

imgnm = db.find({"Imagefilename" : {"$regex" : ".*(?<!jpg)$","$options" : "i"},"Imagefilename" : {"$regex" : "*(?<!jpeg)$","$options" : "i"},"Imagefilename" : {"$regex" : "*(?<!png)$","$options" : "i"}})


img = db.find({"Imagefilename": re.compile(".*(?<!jpg)$", re.IGNORECASE)})