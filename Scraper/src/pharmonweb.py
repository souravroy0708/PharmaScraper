#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import urllib
import pymongo
import logging
import requests
import httplib2
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class pharmonweb(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler(
            "logs/" + config['template'] + "_" + config["site"].replace("/", "_").replace(".", "_").replace(":","") + ".log")
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

    def get_megacatgorylinks(self, soup):
        megacatlist = []
        itr=0
        for item in soup.find("ul", {"class": "nav-inner"}).find_all("li", recursive=False)[0:2]:
            if (not(item.find("a") == None)):
                itr=itr+1
                megacatdict = dict()
                megacatdict[item.find("a").text.strip()] = self.config['site'] + item.find("a")['href']
                if (itr==2):
                    megacatdict[item.find("a").text.strip()] = megacatdict[item.find("a").text.strip()] +"/2"
                megacatlist.append(megacatdict)
        return megacatlist


    def is_product(self, url):
        soup = self.get_soup(httplib2.iri2uri(url))
        try:
            isnotdisabled = not(soup.find("span",{"class":"disabled"})==None)
            return(isnotdisabled)
        except:
            return(False)


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
        run=True
        while run:
            soup = self.get_soup(httplib2.iri2uri(url + "/" + str(pgid)))
            prods = soup.find("div",{"class":"grid-product"}).find_all('div',recursive=False)
            self.logger.info("#Found products:" + str(len(prods)))
            for prod in prods:
                try:
                    proddict = dict()
                    proddict['Source'] = config['site']
                    proddict['Mega-category'] = config['Mega-category']
                    proddict['Category'] = prod.find("span", {"class": "theme"}).text.strip()
                    proddict['segment'] = config['segment']
                    proddict['Sub-segment'] = config['Sub-segment']
                    proddict['template'] = config['template']
                    try:
                        proddict['Product_name'] = prod.find("div", {"class": "product-name"}).text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['Brand'] = prod.find("span", {"class": "laboratory"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 139:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['Price'] = float(prod.find("p",{"class":"price-promote text-center"}).find_all("span",recursive=False)[1].text.strip().replace("€","").replace(",","."))
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['Price'] = "None"
                    try:
                        proddict['Imagelink'] = prod.find("div", {"class": "product-image"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[
                            len(proddict['Imagelink'].split("/")) - 1]
                    except Exception as e:
                        self.logger.error("Line 164:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    try:
                        proddict['EAN7'] = proddict['Imagefilename'].split(".")[0]
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['EAN7'] = "None"
                    try:
                        proddict['urltoproduct'] = config['site']+ prod.find("div", {"class": "product-image"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 109:" + str(e))
                        proddict['urltoproduct'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 100:" + str(e))
                    continue
            run = self.is_product(url + "/" + str(pgid))
            pgid = pgid + 1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        # get mega category
        megacatlist = self.get_megacatgorylinks(soup)
        if (len(megacatlist) > 0):
            for megacat in megacatlist:
                config['Mega-category'] = list(megacat.keys())[0]
                url = megacat[config['Mega-category']]
                config['Category'] = "None"
                config['Sub-segment'] = "None"
                config['segment'] = "None"
                try:
                    self.get_proddata(url)
                except Exception as e:
                    self.logger.error("Line 100:" + str(e))
                    continue
        else:
            config['Mega-category'] = "None"
            config['Category'] = "None"
            config['Sub-segment'] = "None"
            config['segment'] = "None"
            try:
                self.get_proddata(url)
            except Exception as e:
                self.logger.error("Line 100:" + str(e))
        pass

