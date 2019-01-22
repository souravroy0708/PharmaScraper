#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import urllib
import pymongo
import logging
import threading
import requests
import httplib2
from bs4 import BeautifulSoup

# define product page extraction class
class objectifsante(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler(
            config['template'] + "_" + config["site"].replace("/", "_").replace(".", "_").replace(":", "") + ".log")
        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')

    def get_soup(self,url):
        try:
            page = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(page)
        except:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text)
        return (soup)

    def get_menu(soup):
        menudict=dict()
        menudict['pharmacie']=dict()
        menudict['parapharmacie'] = dict()
        for item in soup.find_all("li"):
            if item.find("a")!=None and item.find("a")["id"]=="pharmacieDropdown":
                for anchor in item.find("ul",{"class":"list-unstyled"}).find("li",recursive=False):
                    menudict['pharmacie'][anchor.find("a").text.strip()]=menudict['pharmacie'][anchor.find("a")['href']]
        for item in soup.find_all("li", {"class": "nav-item show"}):
            if (item.find("a") != None):
                if ("parapharmacie" in item.find("a")['href']):
                    catlist.append(self.config["site"] + item.find("a")['href'])
        return catlist



    def is_product(self, url):
        soup = self.get_soup(url)
        return (len(soup.find_all('div', {"itemtype": "http://schema.org/Product"})) > 0)

    def get_proddata(self, url):
        config = self.config
        pgid = 1
        client = pymongo.MongoClient(config["mongolink"])
        db = client['pharmascrape']
        nins = 0
        self.logger.info("Mega-category:" + config['Mega-category'])
        self.logger.info("Category:" + config['Category'])
        self.logger.info("segment:" + config['segment'])
        self.logger.info("Sub-segment:" + config['Sub-segment'])
        while (self.is_product(url + "?&p=" + str(pgid))):
            soup = self.get_soup(url + "?&p=" + str(pgid))
            prods = soup.find_all('div', {"itemtype": "http://schema.org/Product"})
            self.logger.info("#Found products:" + str(len(prods)))
            for prod in prods:
                try:
                    proddict = dict()
                    proddict['Source'] = config['site']
                    proddict['Mega-category'] = config['Mega-category']
                    proddict['Category'] = config['Category']
                    proddict['segment'] = config['segment']
                    proddict['Sub-segment'] = config['Sub-segment']
                    proddict['template'] = config['template']
                    try:
                        proddict['Availability'] = prod.find('p', {"class": "availability stock"}).text
                    except Exception as e:
                        try:
                            self.logger.error("Line 86:" + str(e))
                            proddict['Availability'] = prod.find('p', {"class": "availability stock last"}).text
                        except Exception as e:
                            self.logger.error("Line 90:" + str(e))
                            proddict['Availability'] = "None"
                    try:
                        proddict['Product_name'] = prod.find("a", {"itemprop": "name"}).text
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        try:
                            self.logger.error("Line 95:" + str(e))
                            proddict['Product_name'] = prod.find("span", {"itemprop": "name"}).text
                            if (db['scrapes'].find(
                                    {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                                continue
                        except Exception as e:
                            self.logger.error("Line 99:" + str(e))
                            proddict['Product_name'] = "None"
                    proddict['Format'] = prod.find("div", {"class": "caption"}).text.replace("\n", "").replace(
                        proddict['Product_name'], "").strip()
                    try:
                        proddict['urltoproduct'] = self.config["site"] + \
                                                   prod.find("div", {"class": "caption"}).find("a")['href']
                    except Exception as e:
                        try:
                            self.logger.error("Line 105:" + str(e))
                            proddict['urltoproduct'] = self.config["site"] + \
                                                       prod.find("div", {"class": "caption"}).find("span")['href']
                        except Exception as e:
                            self.logger.error("Line 109:" + str(e))
                            proddict['urltoproduct'] = "None"
                    proddict['Imagelink'] = prod.find("img", {"itemprop": "image"})['src']
                    proddict['Imagefilename'] = prod.find("img", {"itemprop": "image"})['src'].split("/")[
                        len(prod.find("img", {"itemprop": "image"})['src'].split("/")) - 1]
                    try:
                        proddict['Price'] = float(
                            prod.find("p", {"class": "special-price b-hideprice b-blurprice"}).text.replace("\n",
                                                                                                            "").replace(
                                "\xa0€", "").replace(",", ".").strip())
                        proddict['Crossed_out_Price'] = float(
                            prod.find("p", {"class": "old-price b-hideprice b-blurprice"}).text.replace("\n",
                                                                                                        "").replace(
                                "\xa0€", "").replace(",", ".").strip())
                    except Exception as e:
                        self.logger.error("Line 118:" + str(e))
                        proddict['Price'] = float(
                            prod.find("div", {"class": "price-box"}).text.replace("\n", "").replace("\xa0€",
                                                                                                    "").replace(",",
                                                                                                                ".").strip())
                        proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Stars'] = prod.find('div', {"class": "getln"})["data-rating"]
                    except Exception as e:
                        self.logger.error("Line 124:" + str(e))
                        proddict['Stars'] = 0
                    try:
                        proddict['Promotional_format'] = prod.find("p", {"class": "promolot"}).text
                    except Exception as e:
                        self.logger.error("Line 129:" + str(e))
                        proddict['Promotional_format'] = "None"
                    try:
                        proddict['Feature'] = prod.find("p", {"class": "promolottop"}).text
                    except Exception as e:
                        self.logger.error("Line 134:" + str(e))
                        proddict['Feature'] = "None"
                    try:
                        proddict['Discount_claim'] = prod.find("div", {"class": "callout style1 "}).text
                    except Exception as e:
                        self.logger.error("Line 139:" + str(e))
                        proddict['Discount_claim'] = "None"
                    try:
                        prodsoup = self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 149:" + str(e))
                    try:
                        revs = prodsoup.find("div", {"id": "reviews"}).find_all("div", {"class": "row review"})
                        revlist = []
                        for rev in revs:
                            revdict = dict()
                            revdict['author'] = rev.find("span", {"itemprop": "author"}).text
                            revdict['date'] = rev.find("span", {"class": "date"}).text
                            revdict['revhead'] = rev.find("strong", {"itemprop": "name"}).text
                            revdict['revtext'] = rev.find("p", {"itemprop": "description"}).text
                            revdict['rating'] = rev.find("div", {"class": "col-sm-9"}).find("div")["data-rating"]
                            revlist.append(revdict)
                        proddict["Reviews"] = revlist
                    except Exception as e:
                        self.logger.error("Line 148:" + str(e))
                        proddict["Reviews"] = "None"
                    try:
                        proddict["NumReviews"] = prodsoup.find("span", {"itemprop": "reviewCount"}).text
                    except Exception as e:
                        self.logger.error("Line 160:" + str(e))
                        proddict["NumReviews"] = 0
                    try:
                        if ("Marque" in prodsoup.find("div", {"itemtype": "http://schema.org/Product"}).find("p").text):
                            proddict["Brand"] = prodsoup.find("div", {"itemtype": "http://schema.org/Product"}).find(
                                "p").text.replace("Marque : ", "").strip()
                        else:
                            proddict["Brand"] = "None"
                    except Exception as e:
                        self.logger.error("Line 165:" + str(e))
                        proddict["Brand"] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 79:" + str(e))
                    continue
            pgid = pgid + 1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        # get segments
        if (config['Mega-category'] == "Medicament"):
            config['Category'] = "Médicaments"
            allseg = self.get_allseg(soup)
            for key in list(allseg.keys()):
                url = allseg[key]['url']
                config['segment'] = key
                allsubseg = self.get_allsubseg(soup)
                if (len(allsubseg) == 0):
                    config['Sub-segment'] = "None"
                    self.config = config
                    self.get_proddata(url)
                else:
                    for subseg in list(allsubseg.keys()):
                        config['Sub-segment'] = subseg
                        self.config = config
                        self.get_proddata(allsubseg[subseg]['url'])
        else:
            catlist = self.get_catgorylinks(soup)
            for cat in catlist:
                config['Category'] = '-'.join(cat.split("/")[len(cat.split("/")) - 1].split("-")[:-1])
                soup = self.get_soup(cat)
                allseg = self.get_allseg(soup)
                for seg in list(allseg.keys()):
                    url = allseg[seg]['url']
                    config['segment'] = seg
                    allsubseg = self.get_allsubseg(self.get_soup(url))
                    if (len(allsubseg) == 0):
                        config['Sub-segment'] = "None"
                        self.config = config
                        self.get_proddata(url)
                    else:
                        for subseg in list(allsubseg.keys()):
                            config['Sub-segment'] = subseg
                            self.config = config
                            self.get_proddata(allsubseg[subseg]['url'])
        pass





config = dict()
config['template'] = "pharmaplay"
config["mongolink"] = "mongodb://pharmaadmin:pharmafrpwdd@localhost:27017/pharmascrape"
config["site"] = "https://www.pharmaciepolygone.com/fr"
config["urls"] = config["site"] + "/"

