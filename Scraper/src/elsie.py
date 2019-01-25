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
import re
from bs4 import BeautifulSoup

# define product page extraction class
class elsie(threading.Thread):
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

    def get_menu(self,soup):
        menudict=dict()
        for item in soup.find_all("li",{"class":"level0"}):
            try:
                if item.find("a")!=None:
                    menudict[item.find("a", {"class": "level-top"}).find("span").text.strip()] = dict()
                    try:
                        if (len(item.find("ul").find_all("li",{"class":"level1"},recursive=False))>0):
                            for elem in item.find("ul").find_all("li",{"class":"level1"},recursive=False):
                                menudict[item.find("a", {"class": "level-top"}).find("span").text.strip()][
                                    elem.find("a").text.strip()]=dict()
                                menudict[item.find("a", {"class": "level-top"}).find("span").text.strip()][elem.find("a").text.strip()]['url']=elem.find("a")['href']
                        else:
                            menudict[item.find("a", {"class": "level-top"}).find("span").text.strip()]['url'] = item.find("a", {"class": "level-top"})['href']
                    except:
                        menudict[item.find("a", {"class": "level-top"}).find("span").text.strip()]['url']= item.find("a", {"class": "level-top"})['href']
            except:
                continue
        return menudict


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
            soup = self.get_soup(url + "?p=" + str(pgid))
            prods = soup.find('ul', {"class": "products-grid"}).find_all("li",recursive=False)
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
                        proddict['Product_name'] = prod.find("h2",{"class":"product-name"}).find("a").text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 98:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = prod.find("h2",{"class":"product-name"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 106:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        proddict['Brand'] = prod.find("h3",{"class":"marque"}).find("a").text.strip()
                    except Exception as e:
                        self.logger.error("Line 111:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['Price'] = float(prod.find("p", {"class": "special-price"}).find("span", {"class": "price"}).text.replace("\xa0€","").replace(",",".").strip())
                        proddict['Crossed_out_Price'] = float(prod.find("p", {"class": "old-price"}).find("span", {"class": "price"}).text.replace("\xa0€","").replace(",",".").strip())
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['Price'] = float(prod.find("span", {"class": "price"}).text.replace("\xa0€","").replace(",",".").strip())
                        proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Imagelink'] = prod.find("a",{"class":"product-image"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    except Exception as e:
                        self.logger.error("Line 123:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    try:
                        proddict['Availability'] = prod.find("button", {"type": "submit"}).find("span").text.strip()
                    except Exception as e:
                        self.logger.error("Line 130:" + str(e))
                        proddict['Availability'] = "None"
                    try:
                        proddict['Promotional_Claim'] = prod.find("span", {"class": "promotion"}).find("span").text.strip()
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
            if (len(soup.find_all("a",{"class":"next i-next"}))==0):
                run=False
            pgid = pgid + 1
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
            if ('url' in list(catdict.keys())):
                config['segment'] = "None"
                self.get_proddata(catdict['url'])
            else:
                for key2 in list(catdict.keys()):
                    config['segment'] = key2
                    segdict = catdict[key2]
                    if ('url' in list(segdict.keys())):
                        self.get_proddata(segdict['url'])
        pass

