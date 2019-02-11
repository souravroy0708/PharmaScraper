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
import re
from bs4 import BeautifulSoup


# define product page extraction class
class groupuniverspharmacie(threading.Thread):
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


    def get_catgorylinks(self, soup):
        catlist = []
        try:
            for item in soup.find("ul", {"class": "upmenu"}).find_all("li",recursive=False):
                if (not (item.find("a") == None)):
                    catdict = dict()
                    catdict[item.find("a").text.strip()] = self.config['site']+item.find("a")['href']
                    catlist.append(catdict)
            catlist=catlist[1:]
        except Exception as e:
            self.logger.info("Error:" + str(e))
            self.logger.info("No Categories")
        return catlist

    def get_allseg(self, soup):
        seglist = []
        try:
            for item in soup.find("ul", {"class": "level1"}).find_all("li",  recursive=False):
                if (not (item.find("a") == None)):
                    segdict = dict()
                    segdict[item.find("a").text.strip()] = self.config['site']+ item.find("a")['href']
                    seglist.append(segdict)
        except:
            return seglist
        return seglist

    def get_allsubseg(self, soup):
        subseglist = []
        try:
            for item in soup.find("ul", {"class": "level2"}).find_all("li",  recursive=False):
                if (not (item.find("a") == None)):
                    subsegdict = dict()
                    subsegdict[item.find("a")['title'].strip()] = self.config['site']+ item.find("a")['href']
                    subseglist.append(subsegdict)
        except:
            return subseglist
        return subseglist



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
        soup = self.get_soup(httplib2.iri2uri(url))
        prods = soup.find_all('div', {"class": "prod_list"})
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
                    proddict['Product_name'] = prod.find("p", {"class": "prod_title"}).text.strip()
                    if (db['scrapes'].find(
                            {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                        continue
                except Exception as e:
                    self.logger.error("Line 116:" + str(e))
                    proddict['Product_name'] = "None"
                try:
                    proddict['Price'] = float(prod.find("p", {"class": "prod_price"}).text.replace("€","").replace(",",".").strip())
                except Exception as e:
                    self.logger.error("Line 154:" + str(e))
                    proddict['Price'] = "None"
                try:
                    proddict['urltoproduct'] = self.config['site']+prod.find("a")["href"]
                except Exception as e:
                    self.logger.error("Line 129:" + str(e))
                    proddict['urltoproduct'] = "None"
                try:
                    proddict['Imagelink'] = "//".join(self.config['site'].split("/")[:-1]).replace("////","//") + prod.find("div",{"class":"img_cont"}).find("img")["src"]
                    proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/")) - 1]
                except Exception as e:
                    self.logger.error("Line 164:" + str(e))
                    proddict['Imagelink'] = "None"
                    proddict['Imagefilename'] = "None"
                try:
                    proddict['Format'] = re.search(".\d+ ml",proddict['Product_name']).group(0).strip()
                except Exception as e:
                    self.logger.error("Line 144:"+str(e))
                    proddict['Format'] = "None"
                db['scrapes'].insert_one(proddict)
                nins = nins + 1
                self.logger.info("#insertions:" + str(nins))
            except Exception as e:
                self.logger.info("soup:" + str(prod))
                self.logger.error("Line 100:" + str(e))
                continue
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        # get mega category
        catlist = self.get_catgorylinks(soup)
        if (len(catlist) > 0):
            for cat in catlist:
                config['Category'] = list(cat.keys())[0]
                url = cat[config['Category']]
                soup = self.get_soup(url)
                seglist = self.get_allseg(soup)
                if (len(seglist) > 0):
                    for seg in seglist:
                        config['segment'] = list(seg.keys())[0]
                        url = seg[config['segment']]
                        soup = self.get_soup(url)
                        subseglist = self.get_allseg(soup)
                        if (len(subseglist) > 0):
                            for subseg in subseglist:
                                config['Sub-segment'] = list(subseg.keys())[0]
                                url = subseg[config['Sub-segment']]
                                self.get_proddata(url)
                        else:
                            config['Sub-segment'] = "None"
                            self.get_proddata(url)
                else:
                    config['Sub-segment'] = "None"
                    config['segment'] = "None"
                    self.get_proddata(url)
        else:
            config['Category'] = "None"
            config['Sub-segment'] = "None"
            config['segment'] = "None"
            self.get_proddata(url)
        pass


