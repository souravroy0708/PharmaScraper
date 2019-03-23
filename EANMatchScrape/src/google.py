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
        url = 'https://www.google.fr/search?q=reference%20"' + "%20".join(prodname.split(" ")) + '"'
        urllist = []
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
        try:
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.content)
            for link in soup.find_all("a"):
                if (link.has_attr('href')):
                    if ("https://" in link['href'] and "webcache" not in link['href'] and "google." not in link['href'] and "youtube." not in link['href']):
                        templink = link['href'].split("&")[0]
                        if ("https:" in templink):
                            urllist.append("http" +templink.split("http")[1])
            self.logger.info("Numbe rof links received:" + str(len(urllist)))
        except:
            self.logger.info("Failed prod:" + prodname)
        return (urllist)

    def get_ean_regex(self,urllist,regex=r"\b(\d{7}|\d{13})\b"):
        eangllist = []
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
        for url in urllist:
            try:
                page = requests.get(url, headers=headers)
                soup = BeautifulSoup(page.content)
                textsite = soup.text
                eanmatchurltext = list(set(re.findall(regex, textsite)))
                eanmatchurlurl = list(set(re.findall(regex, url)))
                if (len(eanmatchurltext)>0 or len(eanmatchurlurl)>0):
                    eandict=dict()
                    eandict['url'] = url
                    if (len(eanmatchurltext)>0):
                        eandict['eanmatchurltext'] = eanmatchurltext
                    if (len(eanmatchurlurl)>0):
                        eandict['eanmatchurlurl'] = eanmatchurlurl
                    eangllist.append(eandict)
                else:
                    continue
            except Exception as e:
                self.logger.info("Error in parsing regex:" + str(e))
                continue
        return(eangllist)


    def run(self):
        run = True
        while run:
            try:
                client = pymongo.MongoClient(self.config["mongolink"])
                db = client[self.config["db"]]
                cursor = db[self.config["targetcollection"]].find({"$and":[{"EAN13":{ "$exists": False }},{"EAN7":{ "$exists": False}},{"googleean":{ "$exists": False}}]},no_cursor_timeout=True)
                for doc in cursor:
                    prodname = doc["Product_name"]
                    self.logger.info("STarted prod:" + prodname)
                    urllist = self.get_search_links(prodname)
                    eanlist = self.get_ean_regex(urllist)
                    if (len(eanlist)>0):
                        db[self.config["targetcollection"]].update_one({"_id": doc['_id']},{"$set": {"googleean" : eanlist}})
                    else:
                        db[self.config["targetcollection"]].update_one({"_id": doc['_id']},{"$set": {"googleean": ["NotFound"]}})
            except Exception as e:
                print(str(e))
                client.close()
                count = db[self.config["targetcollection"]].find({"$and":[{"EAN13":{ "$exists": False }},{"EAN7":{ "$exists": False}},{"googleean":{ "$exists": False}}]}).count()
                if (count==0):
                    run = False
        pass

