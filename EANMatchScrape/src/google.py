#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import requests
import pymongo
import logging
import threading
import re
from bs4 import BeautifulSoup


# define product page extraction class
class googlesearch(threading.Thread):
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

    def get_search_links(self):
        url = "https://www.google.dz/search?q=" + self.config['prod']
        urllist = []
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content)
            for link in soup.find_all("a", href=re.compile("(?<=/url\?q=)(htt.*://.*)")):
                urllist.extend(re.split(":(?=http)", link["href"].replace("/url?q=", "")))
        except:
            self.logger.info("Failed url:" + self.config['prod'])
        return (urllist)

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