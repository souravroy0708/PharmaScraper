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
class universpharmacie(threading.Thread):
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
            for item in soup.find("div", {"id": "main_menu"}).find("table").find("tr").find_all("td",recursive=False):
                if (not (item.find("a") == None) and "http" in item.find("a")['href']):
                    catdict = dict()
                    catdict[item.find("a").text.strip()] = item.find("a")['href']
                    catlist.append(catdict)
        except Exception as e:
            self.logger.info("Error:" + str(e))
            self.logger.info("No Categories")
        return catlist

    def get_allseg(self, soup):
        seglist = []
        for item in soup.find("ul", {"class": "inline_list"}).find_all("li",  recursive=False):
            if (not (item.find("a") == None)):
                segdict = dict()
                segdict[item.find("a")['title'].strip()] =  item.find("a")['href']
                seglist.append(segdict)
        return seglist

    def get_allsubseg(self, soup):
        subseglist = []
        for item in soup.find("ul", {"class": "inline_list"}).find_all("li", recursive=False):
            if (not (item.find("a") == None)):
                subsegdict = dict()
                subsegdict[item.find("a")['title'].strip()] = item.find("a")['href']
                subseglist.append(subsegdict)
        return subseglist


    def is_product(self, url):
        soup = self.get_soup(httplib2.iri2uri(url))
        try:
            isnotdisabled = not(soup.find("li",{"id":"pagination_next"})["class"].strip() == "disabled")
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
            soup = self.get_soup(httplib2.iri2uri(url + "?p=" + str(pgid)))
            prods = soup.find_all('div', {"class": "shadow_box_1"})
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
                        proddict['Product_name'] = prod.find("div", {"class": "flag_block_3"})['title']
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['Availability'] = prod.find("div", {"id": "list_availability"}).find("span").text.strip()
                    except Exception as e:
                        self.logger.error("Line 124:" + str(e))
                        proddict['Availability'] = "None"
                    try:
                        proddict['urltoproduct'] = prod.find("a",recursive=False)['href']
                    except Exception as e:
                        self.logger.error("Line 129:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        prodsoup = self.get_soup(httplib2.iri2uri(proddict['urltoproduct']))
                    except:
                        self.logger.error("Line 134:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['Brand'] = prodsoup.find("div",{"id":"center_column"}).find_all("div",{"class":"block_0"},recursive=False)[2].text.replace(" Voir tous les produits de la marque ","").strip().split("\n")[0]
                    except Exception as e:
                        self.logger.error("Line 139:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['Format'] = re.search(".\d+ ml",prod.find("div", {"class": "flag_block_3"})['title']).group(0).strip()
                    except Exception as e:
                        self.logger.error("Line 144:"+str(e))
                        proddict['Format'] = "None"
                    try:
                        proddict['EAN13'] = proddict['urltoproduct'].split("-")[-1:][0].split(".")[0]
                    except Exception as e:
                        self.logger.error("Line 149:"+str(e))
                        proddict['EAN13'] = "None"
                    try:
                        proddict['Price'] = float(prod.find("div", {"itemprop": "price"})['content'].strip())
                    except Exception as e:
                        self.logger.error("Line 154:" + str(e))
                        proddict['Price'] = "None"
                    try:
                        proddict['Loyalty'] = prodsoup.find("div",{"class":"flag_block_4a"}).find("img")["src"]
                    except Exception as e:
                        self.logger.error("Line 159:" + str(e))
                        proddict['Loyalty'] = "None"
                    try:
                        proddict['Imagelink'] = prod.find("div",{"class":"prod_image_block"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/")) - 1]
                    except Exception as e:
                        self.logger.error("Line 164:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 100:" + str(e))
                    continue
            run = self.is_product(url + "?p=" + str(pgid))
            pgid = pgid + 1
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
