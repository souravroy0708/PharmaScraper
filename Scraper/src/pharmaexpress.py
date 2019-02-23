###declare libraries

import json
import logging
import threading
import urllib
import pymongo
# define chrome options


# define product page extraction class
class pharmaexpress(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler(
            "logs/" + config['template'] + "_" + config["site"].replace("/", "_").replace(".", "_").replace(":",
                                                                                                            "") + ".log")
        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')


    def get_proddata(self,url):
        config=self.config
        run=True
        pgid=1
        while run:
            mediurl = url+str(pgid)
            try:
                prods = json.loads(urllib.request.urlopen(mediurl).read())
            except:
                run=False
            if (type(prods)==list):
                client = pymongo.MongoClient(config["mongolink"])
                db = client[str(config['db'])]
                for prod in prods:
                    proddict=dict()
                    proddict['Source'] = config['site']
                    proddict['Mega-category'] = config['Mega-category']
                    proddict['Category'] = config['Category']
                    proddict['segment'] = config['segment']
                    proddict['Sub-segment'] = config['Sub-segment']
                    proddict['template'] = config['template']
                    proddict['Product_name'] = prod['name']
                    if (db['scrapes'].find({"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                        continue
                    proddict['Brand'] = prod['brand']
                    proddict['Price'] = prod['price']/100
                    proddict['Imagelink'] = prod['image']
                    proddict['Imagefilename'] =  proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    proddict['Availability'] = prod['stock']
                    db[str(config['collection'])].insert_one(proddict)
            else:
                run =False
            pgid = pgid+1
        pass

    def run(self):
        config = self.config
        sites = json.loads(urllib.request.urlopen(config['murl']).read())
        if (len(sites)>0):
            for site in sites:
                config['site']="https://pharma-express.co/pharmacies/"+"_".join(site['name'].split(" "))
                siteid = site['id']
                try:
                    sitemap = json.loads(urllib.request.urlopen("https://api.pharma-express.co/pharmacy/"+str(siteid)+"/categories?type=para").read())
                    config["Mega-category"]="Parapharmacie"
                    try:
                        for cat in sitemap:
                            config["Category"] = cat['name']
                            if (len(cat["subCategories"])>0):
                                for seg in cat["subCategories"]:
                                    config["segment"] = seg['name']
                                    if (len(seg['subCategories'])>0):
                                        for subseg in seg['subCategories']:
                                            config['Sub-segment'] = subseg['name']
                                            url = "https://api.pharma-express.co/pharmacy/"+str(siteid) +"/medicine?categoryID="+subseg['id']+"&type=para&page="
                                            self.config=config
                                            self.get_proddata(url)
                                    else:
                                        config['Sub-segment'] = "None"
                                        url = "https://api.pharma-express.co/pharmacy/" + str(siteid)+ "/medicine?categoryID=" + seg['id'] + "&type=para&page="
                                        self.config = config
                                        self.get_proddata(url)
                            else:
                                config['segment'] = "None"
                                config['Sub-segment'] = "None"
                                url = "https://api.pharma-express.co/pharmacy/" + str(siteid)+ "/medicine?categoryID=" + cat['id'] + "&type=para&page="
                                self.config = config
                                self.get_proddata(url)
                    except Exception as e:
                        self.logger.error("parampharmacie :" + str(e))
                except Exception as e:
                    self.logger.error("parampharmacie :" + str(e))
                try:
                    sitemap = json.loads(urllib.request.urlopen("https://api.pharma-express.co/pharmacy/" + str(siteid) + "/categories?type=medi").read())
                    config["Mega-category"] = "Medicament"
                    try:
                        for cat in sitemap:
                            config["Category"] = cat['name']
                            if (len(cat["subCategories"]) > 0):
                                for seg in cat["subCategories"]:
                                    config["segment"] = seg['name']
                                    if (len(seg['subCategories']) > 0):
                                        for subseg in seg['subCategories']:
                                            config['Sub-segment'] = subseg['name']
                                            url = "https://api.pharma-express.co/pharmacy/" + str(siteid) + "/medicine?categoryID=" + \
                                                  subseg['id'] + "&type=para&page="
                                            self.config = config
                                            self.get_proddata(url)
                                    else:
                                        config['Sub-segment'] = "None"
                                        url = "https://api.pharma-express.co/pharmacy/" + str(siteid) + "/medicine?categoryID=" + seg[
                                            'id'] + "&type=para&page="
                                        self.config = config
                                        self.get_proddata(url)
                            else:
                                config['segment'] = "None"
                                config['Sub-segment'] = "None"
                                url = "https://api.pharma-express.co/pharmacy/" + str(siteid) + "/medicine?categoryID=" + cat[
                                    'id'] + "&type=para&page="
                                self.config = config
                                self.get_proddata(url)
                    except Exception as e:
                        self.logger.error("medicament :" + str(e))
                except Exception as e:
                    self.logger.error("medicament :" + str(e))
        pass