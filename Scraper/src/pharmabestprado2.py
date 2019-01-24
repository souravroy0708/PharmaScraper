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
class pharmabestprado2(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler("logs/"+config['template'] + "_" + config["site"].replace("/", "_").replace(".", "_").replace(":", "") + ".log")
        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')

    def get_soup(self, url):
        try:
            page = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(page)
        except:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text)
        return (soup)

    def get_catgorylinks(self, soup):
        catlist = []
        for item in soup.find_all("li", {"class": "sousmenu"}):
            if (item.find("a") != None):
                catdict={}
                catdict[item.find("a").text.strip()]=item.find("a")['href']
                catlist.append(catdict)
        return catlist

    def get_allseg(self, soup):
        segdict = dict()
        for anchor in soup.find('div', {"class": "product_list_area"}).find("ol").find_all("li"):
            if (anchor.find("a") != None):
                segdict[anchor.find("a").text.strip()]=anchor.find("a")['href']
        return segdict


    def is_product(self, url):
        soup = self.get_soup(url)
        numprods = len(soup.find_all('div', {"class": "caps"}))
        return (numprods > 0)

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
        while (self.is_product(url + "PAGE=" + str(pgid))):
            soup = self.get_soup(url + "PAGE=" + str(pgid))
            prods = soup.find_all('div', {"class": "caps"})
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
                        proddict['Product_name'] = prod.find("h2").find("a")["title"].strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                            self.logger.error("Line 99:" + str(e))
                            proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = prod.find("h2").find("a")["href"].strip()
                    except Exception as e:
                        self.logger.error("Line 105:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        prodsoup = self.get_soup(httplib2.iri2uri(proddict['urltoproduct']))
                    except:
                        self.logger.error("Line 114:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict["Brand"] = prodsoup.find("div", {"class": "brand_area"}).find("a")['title'].replace("Voir tous les articles de la marque ","").strip()
                    except Exception as e:
                        self.logger.error("Line 165:" + str(e))
                        proddict["Brand"] = "None"
                    try:
                        proddict['Format'] = prodsoup.find("span", {"class": "ui-selectmenu-text"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['Format '] = "None"
                    try:
                        proddict['Price'] = float(prod.find("div", {"class": "price"}).text.replace("€","").replace(",", ".").strip())
                    except Exception as e:
                        self.logger.error("Line 118:" + str(e))
                        proddict['Price'] = "None"
                    try:
                        proddict['Imagelink'] = config['site'] + prodsoup.find("div", {"class": "bloc_image"}).find("a")['href']
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/")) - 1]
                    except:
                        self.logger.error("Line 86:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
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
        try:
            catlist=self.get_catgorylinks(soup)
        except:
            catlist=[]
        if (len(catlist)>0):
            for cat in catlist:
                config['Category']=list(cat.keys())[0]
                url=cat[config['Category']]
                soup = self.get_soup(httplib2.iri2uri(url))
                try:
                    allseg= self.get_allseg(soup)
                except:
                    allseg= dict()
                if (len(allseg)>0):
                    for seg in list(allseg.keys()):
                        url = allseg[seg]
                        config['segment']=seg
                        soup = self.get_soup(httplib2.iri2uri(url))
                        try:
                            allsubseg= self.get_allseg(soup)
                        except:
                            allsubseg= dict()
                        if (len(allsubseg)>0):
                            for subseg in list(allsubseg.keys()):
                                url = allsubseg[subseg]
                                config['Sub-segment']=subseg
                                self.get_proddata(url)
                        else:
                            config['Sub-segment']="None"
                            self.get_proddata(url)
                else:
                    config['segment']="None"
                    config['Sub-segment']="None"
                    self.get_proddata(url)
        else:
            config['Category']="None"
            config['segment']="None"
            config['Sub-segment']="None"
            self.get_proddata(url)
        pass

