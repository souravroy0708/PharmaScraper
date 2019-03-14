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
            "logs/google.log")
        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')

    def get_search_links(self,prodname):
        url = "https://www.google.com/search?q=" + "%20".join(prodname.split(" "))
        urllist = []
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content)
            for link in soup.find_all("a"):
                if (link.has_attr('href')):
                    if ("https://" in link['href'] and "webcache" not in link['href'] and "google." not in link['href'] and "youtube." not in link['href']):
                        templink = link['href'].split("&")[0]
                        if ("https:" in templink):
                            urllist.append("http" +templink.split("http")[1])
        except:
            self.logger.info("Failed prod:" + prodname)
        return (urllist)

    def get_ean_regex(urllist,regex=r"\b(\d{7}|\d{13})\b"):
        eangllist = []
        for url in urllist:
            page = requests.get(url)
            soup = BeautifulSoup(page.content)
            textsite = soup.text
            eanmatch = list(set(re.findall(regex, textsite)))
            if (len(eanmatch)>0):
                eandict=dict()
                eandict['url'] = url
                eandict['eanmatch'] = eanmatch
                eangllist.append(eandict)
            else:
                continue
        return(eangllist)


    def run(self):
        run = True
        while run:
            try:
                client = pymongo.MongoClient(self.config["mongolink"])
                db = client[self.config["db"]]
                cursor = db[self.config["targetcollection"]].find({"$or":[{"EAN13":{ "$exists": False }},{"EAN7":{ "$exists": False}}]},no_cursor_timeout=True)
                for doc in cursor:
                    prodname = doc["Product_name"]
                    urllist = self.get_search_links(prodname)
                    eanlist = self.get_ean_regex(urllist)
                    if (len(eanlist)>0):
                        db[self.config["targetcollection"]].update_one({"_id": doc['_id']},{"$set": {"googleean" : eanlist}})
                    else:
                        db[self.config["targetcollection"]].update_one({"_id": doc['_id']},{"$set": {"googleean": ["NotFound"]}})
            except:
                client.close()
                count = db[self.config["targetcollection"]].find({"$or":[{"EAN13":{ "$exists": False }},{"EAN7":{ "$exists": False}},{"googleean":{ "$exists": False}}]},no_cursor_timeout=True).count()
                if (count==0):
                    run = False

