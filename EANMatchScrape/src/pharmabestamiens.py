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
class pharmabestamiensean(threading.Thread):
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
            url = self.config['site'] + self.config['urlsuffix'] + self.config['ean']
            try:
                page = urllib.request.urlopen(url).read()
                soup = BeautifulSoup(page)
            except:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
                r = requests.get(url, headers=headers)
                soup = BeautifulSoup(r.text)
            retdict = dict()
            if (soup.find("div",{"class":"cat-desc"}).find("strong").text.strip()=="n’a trouvé aucun résultat"):
                return(retdict)
            retdict['url'] = self.config['site'] + soup.find("div", {"class": "texts"}).find_all("a")[-1:][0]['href']
            retdict['product'] = soup.find("div", {"class": "title"}).text.strip()
            retdict['site'] = self.config['site']
            retdict['image'] = soup.find("div", {"class": "image"}).find("img")['src']
            retdict['ean'] = self.config['ean']
            retdict['template'] = self.config['template']
        except Exception as e:
            self.logger.info("Failed url:" + self.config['site'])
            self.logger.info("Failed ean:" + self.config['ean'])
            retdict = dict()
        return (retdict)

    def run(self):
        for site in self.config['sites']:
            self.config['site'] = site
            for ean in self.config['eanlist']:
                self.config['ean'] = str(ean)
                client = pymongo.MongoClient(self.config["mongolink"])
                db = client[self.config["db"]]
                if (db[self.config["collection"]].find(
                        {"site": self.config['site'], "ean": self.config['ean']}).count() > 0):
                    self.logger.info("Success url:" + self.config['site'])
                    self.logger.info("Success ean:" + self.config['ean'])
                    continue
                else:
                    retdict = self.get_search_res()
                    if (len(retdict) > 0):
                        db[self.config["collection"]].insert_one(retdict)
                        self.logger.info("Success url:" + self.config['site'])
                        self.logger.info("Success ean:" + self.config['ean'])
                client.close()
        pass
