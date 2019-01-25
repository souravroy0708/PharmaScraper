#custom scraper classes
from src.aprium import aprium
from src.boticinal import boticinal
from src.shoppharmacie import shoppharmacie
from src.pharmanity import pharmanity
from src.citypharma import citypharma
from src.pharmaviewol import pharmaviewol
from src.lafayette_1 import lafayette_1
from src.lafayette_2 import lafayette_2
from src.mesoigner import mesoigner
from src.cocooncenter import cocooncenter
from src.gpo import gpo
from src.pharmabestpraden import pharmabestpraden
from src.pharmacies1001 import pharmacies1001
from src.newpharma import newpharma
from src.easyparapharmacie import easyparapharmacie
from src.lasante import lasante
from src.santediscount import santediscount
from src.pharmagdd import pharmagdd
from src.pharmabestprado1 import pharmabestprado1
from src.objectifsante import objectifsante
from src.pharmabestamiens import pharmabestamiens
from src.elsie import elsie
from src.mesoignerwithlogin import mesoignerwithlogin
from src.pharmavielogin import pharmavielogin
# import libraries
import random
import pymongo
import json
import threading
import argparse

def main(scrapconfig,nthread):
    configlist=[]
    for template in scrapconfig:
        config = dict()
        for key in list(set(list(template.keys()))-set(['sites','urlsuffix'])):
            config[key] = str(template[key])
        for site in template['sites']:
            try:
                if (site[-1:] == "/"):
                    config["site"] = str(site[:-1])
                else:
                    config["site"] = str(site)
                client = pymongo.MongoClient(config["mongolink"])
                db = client[str(template['db'])]
                sitecount = db[str(template['collection'])].find({"Source": config['site']}).count()
                print(sitecount)
                client.close()
                if (sitecount < 100):
                    config["urls"] = str(config["site"] + template["urlsuffix"])
                    configlist.append(config.copy())
                else:
                    continue
            except Exception as e:
                print(str(e))
                continue
    threadlist=[]
    if (nthread > 0):
        if (len(configlist) > nthread):
            configlist = random.sample(configlist, nthread)
    for config in configlist:
        exec(str("threadlist.append("+config["template"]+"(config))"))
    for thread in threadlist:
        thread.start()
    for thread in threadlist:
        thread.join()

if __name__ == "__main__":
    with open('./conf/scrapconfig.json') as jsonfile:
        scrapconfig = json.load(jsonfile)
    parser = argparse.ArgumentParser()
    parser.add_argument('--whichtemplate', dest='whichtemplate', default="lasante",help='mention the class name to run')
    parser.add_argument('--numthreads', dest='numthreads', default=10,help='how many threads')
    args = parser.parse_args()
    if (args.whichtemplate != "all"):
        confdict=[]
        for item in scrapconfig:
            if (item['template'] == args.whichtemplate):
                confdict.append(item)
            else:
                continue
        scrapconfig = confdict
    main(scrapconfig,int(args.numthreads))



