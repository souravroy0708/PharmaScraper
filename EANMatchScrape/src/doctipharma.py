#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import platform
import pymongo
import logging
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class pharmarketean(threading.Thread):
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

    def get_search_res(self):
        try:
            # define chrome options
            chrome_options = Options()
            chrome_options.add_argument('--dns-prefetch-disable')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--lang=en-US')
            # chrome_options.add_argument('--headless')
            if (platform.system() == "Darwin"):
                driver = webdriver.Chrome("./chromedrivers/chromedriver_mac",
                                          chrome_options=chrome_options)
            elif (platform.system() == "Linux"):
                driver = webdriver.Chrome("./chromedrivers/chromedriver_linux",chrome_options=chrome_options)
            else:
                driver = webdriver.Chrome("./chromedrivers/chromedriver.exe",
                                          chrome_options=chrome_options)
            driver.get(self.config['site'])
            ean_input = driver.find_element_by_tag_name("input")
            ean_input.send_keys(self.config['ean'])
            time.sleep(2)
            soup=BeautifulSoup(driver.page_source)
            retdict=dict()
            retdict['url'] = soup.find("div",{"class":"df-card"}).find("a")["href"]
            retdict['product']=soup.find("div",{"class":"df-card__title"}).text.strip()
            retdict['site'] = self.config['site']
            retdict['image'] = "https://"+soup.find("figure",{"class":"df-card__image"}).find("img")["src"]
            driver.quit()
        except Exception as e:
            self.logger.info("url:" + self.config['site'])
            self.logger.info("ean:" + self.config['ean'])
            retdict=dict()
        return (retdict)

    def run(self):
        for site in self.config['sites']:
            self.config['site'] = site
            retdict = self.get_search_res(self)
            if (len(retdict)>0):
                client = pymongo.MongoClient(self.config["mongolink"])
                db = client[self.config["db"]]
                db[self.config["collection"]].insert_one(retdict)
        pass
