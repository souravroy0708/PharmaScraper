#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import pymongo
import logging
import threading
import platform
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# define product page extraction class
class pharmacies1001ean(threading.Thread):
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
            url = self.config['site'] + self.config['urlsuffix'] + self.config['ean'] + "&page=1"
            chrome_options = Options()
            chrome_options.add_argument('--dns-prefetch-disable')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--lang=en-US')
            chrome_options.add_argument('--headless')
            if (platform.system() == "Darwin"):
                driver = webdriver.Chrome("./chromedrivers/chromedriver_mac",
                                          chrome_options=chrome_options)
            elif (platform.system() == "Linux"):
                driver = webdriver.Chrome("./chromedrivers/chromedriver_linux", chrome_options=chrome_options)
            else:
                driver = webdriver.Chrome("./chromedrivers/chromedriver.exe",
                                          chrome_options=chrome_options)
            driver.get(self.config['site'])
            soup=BeautifulSoup(driver.page_source)
            retdict = dict()
            retdict['url'] = self.config['site'] + "/" + soup.find("h2", {"class": "title order-1 mb-0"}).find("a")['href']
            retdict['product'] = soup.find("h2", {"class": "title order-1 mb-0"}).find("a").text.strip()
            retdict['site'] = self.config['site']
            retdict['image'] = soup.find("div", {"class": "illus"}).find("img")['src']
            retdict['ean'] = self.config['ean']
            retdict['template'] = self.config['template']
            driver.quit()
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
