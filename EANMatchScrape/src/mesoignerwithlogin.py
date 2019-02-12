#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import requests
import urllib
import pymongo
import logging
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class mesoignerwithloginean(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler(
            "logs/" + config['template'] + ".log")
        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')

    def get_search_res(self):
        try:
            url = self.config['site']  + self.config['urlsuffix'] + self.config['ean']
            try:
                page = urllib.request.urlopen(url).read()
                soup = BeautifulSoup(page)
            except:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
                r = requests.get(url, headers=headers)
                soup = BeautifulSoup(r.text)
            retdict=dict()
            retdict['url'] = self.config['site'] + "/" + soup.find("h2",{"class":"product-title"}).find("a")['href']
            retdict['product']=soup.find("h2",{"class":"product-title"}).find("a").text.strip()
            retdict['site'] = self.config['site']
            retdict['image'] = soup.find("div",{"class":"product-image"}).find("img")['src']
            retdict['ean'] = self.config['ean']
        except Exception as e:
            self.logger.info("url:" + self.config['site'])
            self.logger.info("ean:" + self.config['ean'])
            retdict=dict()
        return (retdict)

    def run(self):
        for site in self.config['sites']:
            self.config['site'] = site
            for ean in self.config['eanlist']:
                self.config['ean'] = ean
                retdict = self.get_search_res()
                if (len(retdict) > 0):
                    client = pymongo.MongoClient(self.config["mongolink"])
                    db = client[self.config["db"]]
                    db[self.config["collection"]].insert_one(retdict)
        pass
