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
from bs4 import BeautifulSoup

# define product page extraction class
class pharmabestamiens(threading.Thread):
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

    def get_categorymenu(self,soup):
        menudict=dict()
        for item in soup.find("ul",{"class":"nav navbar-nav navbar-justified"}).find_all("li",recursive=False):
            if item.find("a") != None:
                menudict[item.find("a").find("span").text.strip()] = dict()
                try:
                    for elem in item.find("ul",{"class":"nav-col-left"}).find_all("li",recursive=False):
                        menudict[item.find("a").find("span").text.strip()][elem.find("a").text.strip()] = self.config['site'] + elem.find("a")['href']
                except:
                    menudict[item.find("a").find("span").text.strip()]=self.config['site'] + item.find("a")['href']
                    continue
        keys = list(menudict.keys())
        for key in keys:
            if "promo" in key or "Marques" in key:
                menudict.pop(key)
        return menudict


    def get_allsubseg(self,soup):
        subseg=dict()
        for item in soup.find("li", {"class": "list-lnk-dx list-lnk-d0 active"}).find("ul").find_all("li",{"class":"list-lnk-dx list-lnk-d1"},recursive=False):
            if item.find("a") != None:
                subseg[item.find("a").find("span").text.strip()] = self.config['site'] + item.find("a")['href']
        return subseg

    def isproduct(self,url):
        soup = self.get_soup(url)
        try:
            gotp = int(soup.find("div",{"class":"pull-right hidden-xs"}).text.strip().split("-")[1].split(" ")[0])
            totalp = int(soup.find("div", {"class": "pull-right hidden-xs"}).text.strip().split("des ")[1].split(" ")[0])
            return (gotp<totalp)
        except:
            return False

    def get_proddata(self, url):
        config = self.config
        pgid = 1
        client = pymongo.MongoClient(config["mongolink"])
        db = client['pharmascrape']
        nins = 0
        run = True
        self.logger.info("Mega-category:" + config['Mega-category'])
        self.logger.info("Category:" + config['Category'])
        self.logger.info("segment:" + config['segment'])
        self.logger.info("Sub-segment:" + config['Sub-segment'])
        while run:
            soup = self.get_soup(url + "/page-" + str(pgid))
            prods = soup.find_all('div', {"class": "product tag-form"})
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
                        proddict['Product_name'] = prod.find("div",{"class":"title"}).text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 98:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['Brand'] = prod.find("a",{"class":"brand"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 111:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['Price'] = float(prod.find("span",{"class":"sale-price"}).text.replace("€",".").strip())
                        proddict['Crossed_out_Price'] = float(prod.find("del",{"class":"old-price"}).text.replace("€",".").strip())
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['Price'] = float(prod.find("span",{"class":"sale-price"}).text.replace("€",".").strip())
                        proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Imagelink'] = config['site']+prod.find("div",{"class":"image"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    except Exception as e:
                        self.logger.error("Line 123:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    try:
                        proddict['Promotional_Claim'] = prod.find("span",{"class":"big2"}).text.strip().replace("€","").replace(",",".")
                        if "%" in proddict['Promotional_Claim']:
                            proddict['Crossed_out_Price'] = proddict['Price']
                            proddict['Price'] = proddict['Price'] * (1-float(proddict['Promotional_Claim'].replace("%","")/100))
                        else:
                            proddict['Crossed_out_Price'] = proddict['Price']
                            proddict['Price'] = proddict['Price'] + float(proddict['Promotional_Claim'])
                    except Exception as e:
                            self.logger.error("Line 135:" + str(e))
                            proddict['Promotional_Claim'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 90:" + str(e))
                    continue
            if (self.isproduct(url + "/page-" + str(pgid))):
                pgid = pgid + 1
            else:
                run=False
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        menudict = self.get_menu(soup)
        for key1 in list(menudict.keys()):
            config['Category'] =key1
            catdict = menudict[key1]
            if (not isinstance(catdict,dict)):
                config['segment'] = "None"
                self.get_proddata(catdict)
            else:
                for key2 in list(catdict.keys()):
                    config['segment'] = key2
                    url = catdict[key2]
                    soup = self.get_soup(url)
                    allsubseg = self.get_allsubseg(soup)
                    for key3 in list(allsubseg.keys()):
                        config['Sub-segment'] = key3
                        self.get_proddata(allsubseg[key3])
        pass

