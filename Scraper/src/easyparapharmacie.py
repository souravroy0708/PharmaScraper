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
import re
from bs4 import BeautifulSoup


# define product page extraction class
class easyparapharmacie(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler("logs/"+config['template'] + "_" + config["site"].replace("/", "_").replace(".", "_").replace(":", "") + ".log")
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
        for item in soup.find("ul",{"class":"nav-primary"}).find_all("li", {"class": "level0"}):
            catdict = dict()
            catdict[item.find("a").text.strip()] = item.find("a")['href']
            catlist.append(catdict)
        return catlist

    def get_allseg(self, soup):
        segdict = dict()
        for anchor in soup.find_all("li", {"itemprop": "itemListElement"}):
            segdict[anchor.find("a").text.strip()] = anchor.find("a")['href'].strip()
        return segdict


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
            soup = self.get_soup(httplib2.iri2uri(url))
            prods = soup.find_all("div", {"class": "product-container"})
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
                        proddict['Product_name'] = prod.find("h2", {"class": "title h3"}).find("a")['title']
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 102:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = prod.find("h2", {"class": "title h3"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 109:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        prodsoup = self.get_soup(httplib2.iri2uri(proddict['urltoproduct']))
                    except:
                        self.logger.error("Line 114:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['Price'] = float(prod.find_all("span", {"class": "price"})[2]['content'])
                        proddict['Crossed_out_Price'] = float(prod.find_all("span", {"class": "price"})[0].text.strip().replace("\xa0€","").replace(",","."))
                    except Exception as e:
                        try:
                            self.logger.error("Line 134:" + str(e))
                            proddict['Price'] = float(prod.find("span", {"class": "price"})['content'])
                            proddict['Crossed_out_Price'] = "None"
                        except Exception as e:
                            self.logger.error("Line 139:" + str(e))
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['EAN13'] = int(prodsoup.find("span", {"itemprop": "sku"}).text.strip())
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['EAN13 '] = 0
                    try:
                        proddict["Discount_claim"] = prod.find("span", {"class": "bullet"}).text.replace(   "\n", "").strip()
                    except Exception as e:
                        self.logger.error("Line 151:" + str(e))
                        proddict["Discount_claim"] = "None"
                    try:
                        proddict['NumReviews'] = int(prodsoup.find("span", {"itemprop": "reviewCount"}).text.strip())
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['NumReviews'] = 0
                    try:
                        proddict['Stars'] = float(prodsoup.find("span", {"itemprop": "ratingValue"}).text.strip())
                    except Exception as e:
                        self.logger.error("Line 124:" + str(e))
                        proddict['Stars'] = "None"
                    try:
                        proddict['Brand'] = prod.find("span", {"class": "brand"}).text.replace("\n", "").strip()
                    except Exception as e:
                        self.logger.error("Line 129:" + str(e))
                        proddict['Brand'] = "None"

                    try:
                        proddict["Promotional_claim"] = prodsoup.find("div", {"class": "js-promotion"}).find("p", {"class": "promotion-txt"}).text.replace("\n", "").strip()
                    except Exception as e:
                        self.logger.error("Line 156:" + str(e))
                        proddict["Promotional_claim"] = "None"
                    try:
                        proddict['Imagelink'] = prodsoup.find("div",{"class":"product-image"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[
                            len(proddict['Imagelink'].split("/")) - 1]
                    except:
                        self.logger.error("Line 161:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    try:
                        try:
                            numr = len(prodsoup.find_all("span", {"itemprop": "author"}))
                            revlist = []
                            for i in range(0,numr):
                                revdict = dict()
                                revdict['author'] = prodsoup.find_all("span",{"itemprop":"author"})[i]
                                revdict['date'] = prodsoup.find_all("meta", {"itemprop": "datePublished"})[i]['content']
                                revdict['revtext'] = prodsoup.find_all("span",{"itemprop":"description"})[i]
                                revdict['rating'] = prodsoup.find_all("span",{"itemprop":"ratingValue"})[i]
                                revlist.append(revdict)
                            proddict["Reviews"] = revlist
                        except Exception as e:
                            self.logger.error("Line 169:" + str(e))
                            proddict["Reviews"] = "None"
                    except Exception as e:
                        self.logger.error("Line 169:" + str(e))
                        proddict["Reviews"] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 94:" + str(e))
                    continue
            try:
                url  = soup.find("a",{"class":"next i-next"})['href']
            except:
                run = False
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        try:
            catlist = self.get_catgorylinks(soup)
        except:
            catlist = []
        if (len(catlist) > 0):
            for cat in catlist:
                config['Category'] = list(cat.keys())[0]
                url = cat[config['Category']]
                soup = self.get_soup(httplib2.iri2uri(url))
                try:
                    allseg = self.get_allseg(soup)
                except:
                    allseg = dict()
                if (len(allseg) > 0):
                    for seg in list(allseg.keys()):
                        url = allseg[seg]
                        config['segment'] = seg
                        soup = self.get_soup(httplib2.iri2uri(url))
                        try:
                            allsubseg = self.get_allseg(soup)
                        except:
                            allsubseg = dict()
                        if (len(allsubseg) > 0):
                            for subseg in list(allsubseg.keys()):
                                url = allsubseg[subseg]
                                config['Sub-segment'] = subseg
                                self.get_proddata(url)
                        else:
                            config['Sub-segment'] = "None"
                            self.get_proddata(url)
                else:
                    config['segment'] = "None"
                    config['Sub-segment'] = "None"
                    self.get_proddata(url)
        else:
            config['Category'] = "None"
            config['segment'] = "None"
            config['Sub-segment'] = "None"
            self.get_proddata(url)
        pass


