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
from bs4 import BeautifulSoup


# define product page extraction class
class pharmagdd(threading.Thread):
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
        for item in soup.find("ul", {"class": "nav navbar-nav menu"}).find_all("li"):
            if (item.find("a") != None):
                catdict={}
                catdict[item.find("a").text.strip()]=item.find("a")['href']
                catlist.append(catdict)
        return catlist

    def get_allseg(self, soup):
        segdict = dict()
        for anchor in soup.findAll('li', {"class": "featuresHeading"}):
            if (anchor.find("ul")==None):
                segdict[anchor.find("a")["title"]] = anchor.find("a")["href"]
            else:
                segdict[anchor.find("ul").find("li").find("a")["title"]] = anchor.find("ul").find("li").find("a")["href"]
        return segdict


    def is_product(self, url):
        soup = self.get_soup(url)
        numprods = len(soup.find_all('div', {"class": "product"}))
        return (numprods > 0)

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
        while (self.is_product(url + "?page=" + str(pgid)+"#products")):
            soup = self.get_soup(url + "?page=" + str(pgid)+"#products")
            prods = soup.find_all('div', {"class": "product"})
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
                        proddict['Availability'] = prod.find('span', {"class": "date-livraison-prod"}).text.strip()
                    except Exception as e:
                        try:
                            self.logger.error("Line 86:" + str(e))
                            proddict['Availability'] = prod.find('p', {"class": "availability stock last"}).text
                        except Exception as e:
                            self.logger.error("Line 90:" + str(e))
                            proddict['Availability'] = "None"
                    try:
                        proddict['Product_name'] = prod.find("p", {"class": "product-title"}).find("a")["title"]
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                            self.logger.error("Line 99:" + str(e))
                            proddict['Product_name'] = "None"
                    try:
                        proddict["Brand"] = prod.find("p",{"class":"product-brand"}).find("a").text.strip()
                    except Exception as e:
                        self.logger.error("Line 165:"+str(e))
                        proddict["Brand"] = "None"
                    try:
                        proddict['Price'] = float(prod.find("div", {"class": "product-price"}).find_all("span")[1].text.replace("\xa0€","").replace(",", ".").strip())
                        proddict['Crossed_out_Price'] = float(prod.find("div", {"class": "product-price"}).find_all("span")[0].text.replace("\xa0€","").replace(",", ".").strip())
                    except Exception as e:
                        self.logger.error("Line 118:" + str(e))
                        proddict['Price'] = float(prod.find("div", {"class": "product-price"}).find_all("span")[0].text.replace("\xa0€","").replace(",", ".").strip())
                        proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['urltoproduct'] = prod.find("p", {"class": "product-title"}).find("a")["href"]
                    except Exception as e:
                        self.logger.error("Line 105:" + str(e))
                        proddict['urltoproduct'] = "None"
                    proddict['Imagelink'] = config['site'] + prod.find("div",{"class":"thumbnail"}).find("a").find("img")['data-src']
                    proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    try:
                        prodsoup = self.get_soup(httplib2.iri2uri(proddict['urltoproduct']))
                    except:
                        self.logger.error("Line 114:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['EAN13'] = int(prod.find("div", {"class": "pull-right reference"}).text.split(":")[1].strip())
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['EAN13 '] = 0
                    try:
                        proddict['NumReviews'] = int(prod.find("ul",{"class":"list list-inline"}).find_all("li",recursive=False)[1].text.split(" ")[0].strip())
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['NumReviews'] = 0
                    try:
                        proddict['Stars'] = float(prodsoup.find("span",{"class":"rating-average"}).text.replace(",",".").strip())
                    except Exception as e:
                        self.logger.error("Line 124:" + str(e))
                        proddict['Stars'] = 0.0
                    try:
                        revs = prodsoup.find_all("div", {"class": "product-rating-comment"})
                        revlist = []
                        for rev in revs:
                            revdict = dict()
                            revdict['author'] = rev.find("div", {"class": "comment-author"}).text.strip()
                            revdict['date'] = rev.find("time")['datetime']
                            revdict['revtext'] = rev.find("div", {"class": "content-comment"}).text.strip()
                            revdict['rating'] = len(rev.find_all("i",{"class":"fa fa-star"}))
                            revlist.append(revdict)
                        proddict["Reviews"] = revlist
                    except Exception as e:
                        self.logger.error("Line 148:" + str(e))
                        proddict["Reviews"] = "None"
                    try:
                        proddict['Promotional_Claim'] = prod.find("div",{"class":"thumbnail"}).find("img",recursive=False)['alt']
                    except Exception as e:
                        self.logger.error("Line 129:" + str(e))
                        proddict['Promotional_Claim'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 79:" + str(e))
                    continue
            pgid = pgid + 1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        try:
            catlist=self.get_catgorylinks(soup)
        except:
            catlist=[]
        if (len(catlist)>0):
            for cat in catlist:
                config['Category']=list(cat.keys())[0]
                url=cat[config['Category']]
                soup = self.get_soup(httplib2.iri2uri(url))
                try:
                    allseg= self.get_allseg(soup)
                except:
                    allseg= dict()
                if (len(allseg)>0):
                    for seg in list(allseg.keys()):
                        url = allseg[seg]
                        config['segment']=seg
                        soup = self.get_soup(httplib2.iri2uri(url))
                        try:
                            allsubseg= self.get_allseg(soup)
                        except:
                            allsubseg= dict()
                        if (len(allsubseg)>0):
                            for subseg in list(allsubseg.keys()):
                                url = allsubseg[subseg]
                                config['Sub-segment']=subseg
                                self.get_proddata(url)
                        else:
                            config['Sub-segment']="None"
                            self.get_proddata(url)
                else:
                    config['segment']="None"
                    config['Sub-segment']="None"
                    self.get_proddata(url)
        else:
            config['Category']="None"
            config['segment']="None"
            config['Sub-segment']="None"
            self.get_proddata(url)
        pass
