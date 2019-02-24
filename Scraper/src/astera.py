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
class astera(threading.Thread):
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
        if(soup.find("ul", {"id": "nav-menu"})==None):
            return megacatlist
        else:
            item  = soup.find("ul", {"id": "nav-menu"}).find_all("li", recursive=False)[4]
            if (not(item.find("a") == None)):
                megacatdict = dict()
                megacatdict[item.find("a").text.strip()] = self.config['site']+item.find("a")['href']
                megacatlist.append(megacatdict)
        return megacatlist


    def get_catgorylinks(self, soup):
        catlist = []
        try:
            for item in soup.find("div", {"class": "fluid"}).find("div").find_all("div",recursive=False):
                if (not (item.find("a") == None) and "http" not in item.find("a")['href']):
                    catdict = dict()
                    catdict[item.find("a").text.strip()] = self.config['site']+item.find("a")['href']
                    catlist.append(catdict)
        except Exception as e:
            self.logger.info("Error:" + str(e))
            self.logger.info("No Categories")
        return catlist

    def get_allseg(self, soup):
        seglist = []
        if (soup.find("div", {"class": "productCategory-block-top"})==None):
            return seglist
        else:
            if (len(soup.find("div", {"class": "productCategory-block-top"}).find("ul"))>0):
                for item in soup.find("div", {"class": "productCategory-block-top"}).find("ul").find_all("li",  recursive=False):
                    if (not (item.find("a") == None)):
                        segdict = dict()
                        segdict[item.find("a").text.strip()] =  item.find("a")['href']
                        seglist.append(segdict)
        return seglist


    def is_product(self, url):
        soup = self.get_soup(httplib2.iri2uri(url))
        if (soup.find("a",{"rel":"next"})==None):
            return False
        else:
            return(len(soup.find("a",{"rel":"next"}))>0)


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
            soup = self.get_soup(httplib2.iri2uri(url + "?page=" + str(pgid)))
            try:
                prods = soup.find_all('div', {"class": "col add-to-cart-form product-details"})
            except:
                prods =  soup.find('div', {"class": "topProducts productCategory-block-right nos-offres-du-mois"}).find_all("form",recursive=True)
            if (len(prods)==0):
                try:
                    prods = soup.find('div', {"id": "products-list"}).find_all("form",recursive=False)
                except:
                    prods = []
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
                        proddict['Product_name'] = prod.find("span", {"class": "topProducts-col-name"}).text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 108:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['Price'] = float(prod.find("div", {"class": "topProducts-col-price"}).text.replace("\xa0€* TTC", "").replace(",", ".").strip())
                        proddict['Crossed_out_Price'] = float(
                            prod.find("div", {"class": "topProducts-col-price-old"}).text.replace("Au lieu de ", "").replace( "\xa0€* TTC", "").replace(",", ".").strip())
                    except Exception as e:
                        self.logger.error("Line 136:" + str(e))
                        try:
                            proddict['Price'] = float(prod.find("div", {"class": "topProducts-col-price"}).text.replace("\xa0€* TTC","").replace(",", ".").strip())
                            proddict['Crossed_out_Price']  = "None"
                        except Exception as e:
                            self.logger.error("Line 141:" + str(e))
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Imagelink'] = config['site']+ soup.find("div",{"class":"topProducts-image"})["style"].split("'")[1].split("&")[0]
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
            run = self.is_product(url + "?page=" + str(pgid))
            pgid = pgid + 1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        # get mega category
        megacatlist = self.get_megacatgorylinks(soup)
        if (len(megacatlist)>0):
            for megacat in megacatlist:
                config['Mega-category'] = list(megacat.keys())[0]
                url = megacat[config['Mega-category']]
                soup = self.get_soup(url)
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
        else:
            config['Mega-category'] = "None"
            config['Category'] = "None"
            config['Sub-segment'] = "None"
            config['segment'] = "None"
            self.get_proddata(url)
        pass

