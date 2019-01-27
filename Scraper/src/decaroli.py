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
class decaroli(threading.Thread):
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
        for item in soup.find("div", {"class": "menu_items"}).find("ul").find_all("li", recursive=False):
            if (not (item.find("a") == None)):
                catdict = dict()
                catdict[item.find("a").text.strip()] = item.find("a")['href']
                catlist.append(catdict)
        return catlist

    def get_allseg(self, soup):
        seglist = []
        for item in soup.find_all("ul",{"class":"menu_sub"}):
            if item.text.startswith(self.config["Category"]):
                for anchor in  item.find("ul",{"class":"list"}).find_all("li"):
                    if (not (anchor.find("a") == None)):
                        segdict = dict()
                        segdict[anchor.find("a").text.strip()] = anchor.find("a")['href']
                        seglist.append(segdict)
        return seglist


    def is_product(self, url):
        soup = self.get_soup(httplib2.iri2uri(url))
        self.logger.info(url)
        try:
            cpg = int(url.split("=")[-1:][0])
            numli = len(soup.find('ul', {"class": "pagination"}).find_all("li",recursive=False))
            npg = int(soup.find('ul', {"class": "pagination"}).find_all("li",recursive=False)[numli - 1].find("a")["href"].split("=")[-1:][0])
            return (cpg < npg)
        except:
            return False

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
            prods = soup.find_all('div', {"class": "product-container"})
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
                        proddict['Product_name'] = prod.find("a", {"class": "product-name"}).text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 108:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = prod.find("a", {"class": "product-name"})['href']
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        proddict['Brand'] = prod.find("span", {"class": "m-title"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 121:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        prodsoup = self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 126:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['Availability'] = prod.find("a",{"title":"Ajouter au panier"})['title']
                    except Exception as e:
                        self.logger.error("Line 131:" + str(e))
                        proddict['Availability'] = "None"
                    try:
                        proddict['Crossed_out_Price'] = float(prodsoup.find("span", {"id": "old_price_display"}).find("span").text.replace("€","").replace(",",".").strip())
                        proddict['Price'] = float(prodsoup.find("span", {"id": "our_price_display"})['content'])
                    except Exception as e:
                        self.logger.error("Line 136:" + str(e))
                        try:
                            proddict['Price'] = float(prodsoup.find("span", {"id": "our_price_display"})['content'])
                            proddict['Crossed_out_Price'] = "None"
                        except Exception as e:
                            self.logger.error("Line 141:" + str(e))
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['EAN13'] = proddict['urltoproduct'].split("-")[-1:][0].split(".")[0]
                    except Exception as e:
                        self.logger.error("Line 148:" + str(e))
                        proddict['EAN13'] = "None"
                    try:
                        proddict['EAN7'] = prodsoup.find_all("span",{"itemprop":"sku"})[1].text
                    except Exception as e:
                        self.logger.error("Line 153:" + str(e))
                        proddict['EAN7'] = "None"
                    try:
                        if (prodsoup.find('p',{"id":"reduction_percent"}).text.strip() == ""):
                            proddict['Discount_claim'] = prodsoup.find('p',{"id":"reduction_percent"}).text.strip()
                        elif (prodsoup.find('p',{"id":"reduction_amount"}).text.strip() == ""):
                            proddict['Discount_claim'] = prodsoup.find('p', {"id": "reduction_amount"}).text.strip()
                        else:
                            proddict['Discount_claim'] = "None"
                    except Exception as e:
                        self.logger.error("Line 158:" + str(e))
                        proddict['Discount_claim'] = "None"
                    try:
                        proddict['Promotional_claim'] = len(prodsoup.find("div",{"id":"image-block"}).find_all('span',{"class":"promo_page"}))>0
                    except Exception as e:
                        self.logger.error("Line 168:" + str(e))
                        proddict['Promotional_claim'] = "None"
                    try:
                        proddict['Imagelink'] = prod.find("img")['src']
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/")) - 1]
                    except Exception as e:
                        self.logger.error("Line 173:" + str(e))
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
        # get segments
        # get segments
        catlist = self.get_catgorylinks(soup)
        if (len(catlist) > 0):
            for cat in catlist:
                config['Category'] = list(cat.keys())[0]
                allseg = self.get_allseg(soup)
                if (len(allseg) > 0):
                    for seg in allseg:
                        config['segment'] = list(seg.keys())[0]
                        config['Sub-segment'] = "None"
                        url = seg[config['segment']]
                        self.get_proddata(url)
                else:
                    config['segment'] = "None"
                    config['Sub-segment'] = "None"
                    url = cat[config['Category']]
                    self.get_proddata(url)
        pass