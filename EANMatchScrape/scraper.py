#custom scraper classes
from src.pharmarket import pharmarketean
from src.doctipharma import doctipharmaean
from src.astera import asteraean
from src.pharmabestamiens import pharmabestamiensean
from src.elsie import elsieean
from src.citypharma import citypharmaean
from src.mesoigner import mesoignerean
from src.mesoignerwithlogin import mesoignerwithloginean
from src.pharmavielogin import pharmavieloginean
from src.pharmaviewol import pharmaviewolean
from src.pharmacies1001 import pharmacies1001ean
from src.google import googlegetean
# import libraries
import pandas as pd
import json
import pymongo
import argparse

def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))

def main(config):
    if (config['fetchean']=="True"):
        eandf = pd.read_csv(config["eanlistfile"])
        configlist = []
        for template in config['eantemplates']:
            template['eanlist'] = eandf['Code Produit']
            configlist.append(template)
        threadlist = []
        for conf in configlist:
            exec(str("threadlist.append(" + conf["template"] + "(conf))"))
        for thread in threadlist:
            thread.start()
        for thread in threadlist:
            thread.join()
    if (config['matchean']=="True"):
        client = pymongo.MongoClient(config["mongolink"])
        db = client[config["db"]]
        cursor = db[config["targetcollection"]].find({"EAN13": { "$exists": False }, "EAN7": {"$exists": False },"eanmatchval": { "$exists": False },}, no_cursor_timeout=True)
        for doc in cursor:
            try:
                prodname = doc['Product_name']
                sitename = doc['Source'].split('/')[2]
                isfound = db[config["sourcecollection"]].find({"product": {"$regex": ".*"+ prodname +".*", "$options": "-i"}}).count()#,"site": {"$regex": ".*"+ sitename +".*", "$options": "-i"}}).count()
                if (isfound>0):
                    matchcursor = db[config["sourcecollection"]].find({"product": {"$regex": ".*"+ prodname +".*", "$options": "-i"}})#,"site": {"$regex": ".*"+ sitename +".*", "$options": "-i"}})
                    eanmatcharray=[]
                    docidarray = []
                    matchsimarray = []
                    for match in matchcursor:
                        eanmatcharray.append(match['ean'])
                        docidarray.append(match['_id'])
                        matchsimarray.append(get_jaccard_sim(prodname, match['product']))
                    db[config["targetcollection"]].update_one({'_id': doc['_id']}, {'$set': {'eanmatchval': eanmatcharray,'docisarray': docidarray,'source': config["sourcecollection"],'confarray':matchsimarray}}, upsert=False)
                    print("Updated")
                else:
                    isfound = db[config["targetcollection"]].find(
                        {"Product_name": {"$regex": ".*" + prodname + ".*", "$options": "-i"}}).count()#,
                         #"Source": {"$regex": ".*" + sitename + ".*", "$options": "-i"},"EAN13": { "$exists": True }}).count()
                    if (isfound>0):
                        matchcursor = db[config["targetcollection"]].find(
                        {"Product_name": {"$regex": ".*" + prodname + ".*", "$options": "-i"}})#,
                         #"Source": {"$regex": ".*" + sitename + ".*", "$options": "-i"},"EAN13": { "$exists": True }})
                        eanmatcharray = []
                        docidarray = []
                        matchsimarray = []
                        for match in matchcursor:
                            eanmatcharray.append(match['EAN13'])
                            docidarray.append(match['_id'])
                            matchsimarray.append(get_jaccard_sim(prodname, match['Product_name']))
                        db[config["targetcollection"]].update_one({'_id': doc['_id']}, {
                            '$set': {'eanmatchval': eanmatcharray, 'docisarray': docidarray,
                                     'source': config["targetcollection"], 'confarray': matchsimarray}}, upsert=False)
                        print("Updated")
                    else:
                        isfound = db[config["targetcollection"]].find(
                            {"Product_name": {"$regex": ".*" + prodname + ".*", "$options": "-i"}}).count()#,
                             #"Source": {"$regex": ".*" + sitename + ".*", "$options": "-i"}, "EAN7": {"$exists": True}}).count()
                        if (isfound>0):
                            matchcursor = db[config["targetcollection"]].find(
                                {"Product_name": {"$regex": ".*" + prodname + ".*", "$options": "-i"}})#,
                                 #"Source": {"$regex": ".*" + sitename + ".*", "$options": "-i"}, "EAN7": {"$exists": True}})
                            eanmatcharray = []
                            docidarray = []
                            matchsimarray = []
                            for match in matchcursor:
                                eanmatcharray.append(match['EAN7'])
                                docidarray.append(match['_id'])
                                matchsimarray.append(get_jaccard_sim(prodname, match['Product_name']))
                            db[config["targetcollection"]].update_one({'_id': doc['_id']}, {
                                '$set': {'eanmatchval': eanmatcharray, 'docisarray': docidarray,
                                         'source': config["targetcollection"], 'confarray': matchsimarray}}, upsert=False)
                            print("Updated")
            except Exception as e:
                print(str(e))
                continue
    if (config['searchgoogle'] == "True"):
        tg = googlegetean(config)
        tg.run()
        pass


if __name__ == "__main__":
    with open('./conf/scrapconfig.json') as jsonfile:
        config = json.load(jsonfile)
    parser = argparse.ArgumentParser(description='Process EAN')
    parser.add_argument("--fetchean",dest='fetchean', type=str, default="True")
    parser.add_argument("--matchean", dest='matchean', type=str, default="False")
    parser.add_argument("--searchgoogle", dest='searchgoogle', type=str, default="False")
    args = parser.parse_args()
    config['fetchean'] = args.fetchean
    config['matchean'] = args.matchean
    config['searchgoogle'] = args.searchgoogle
    main(config)




