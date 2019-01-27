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
class doctipharma(threading.Thread):
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

    def get_megacatgorylinks(self, soup):
        megacatlist = []
        for item in soup.find("ul", {"class": "nav navbar-nav navbar-left"}).find_all("li", recursive=False):
            if (not(item.find("a") == None) and not(item.find("ul") == None) and 'nav' in item['class'][0] and "www" in item.find("a")['href']):
                megacatdict = dict()
                megacatdict[item.find("a").text.strip()] = "https:"+item.find("a")['href']
                megacatlist.append(megacatdict)
        return megacatlist


    def get_catgorylinks(self, soup):
        catlist = []
        for item in soup.find("div", {"id": "rubrique_0_2"}).find("div").find("div").find_all("div", {"class":"accordion-group"},recursive=False):
            if (not (item.find("a") == None)):
                catdict = dict()
                catdict[item.find("a")["title"].strip()] = self.config['site']+item.find("a")['href']
                catlist.append(catdict)
        return catlist

    def get_allseg(self, soup):
        seglist = []
        for item in soup.find("div", {"id": "rubrique_1_3"}).find("div").find("div").find_all("div", {"class": "accordion-group"}, recursive=False):
            if (not (item.find("a") == None)):
                segdict = dict()
                segdict[item.find("a")["title"].strip()] = self.config['site'] + item.find("a")['href']
                seglist.append(segdict)
        return seglist

    def get_allsubseg(self, soup):
        subseglist = []
        for item in soup.find("div", {"id": "rubrique_1_3"}).find("div").find("div").find_all("div", {"class": "accordion-group"}, recursive=False):
            if (not (item.find("a") == None) and item.find("a")["title"].strip() == self.config['segment']):
                if (len(item.find_all("div",{"id":"rubrique_2_5"}))>0):
                    for elem in item.find("div",{"id":"rubrique_2_5"}).find("div").find("div").find_all("div", {"class": "accordion-group"}, recursive=False):
                        subsegdict = dict()
                        subsegdict[elem.find("a")["title"].strip()] = self.config['site'] + elem.find("a")['href']
                        subseglist.append(subsegdict)
        return subseglist


    def is_product(self, url):
        soup = self.get_soup(httplib2.iri2uri(url))
        self.logger.info(url)
        try:
            cpg = int(url.split("=")[-1:][0])
            numli = len(soup.find('ul', {"class": "pages"}).find_all("li",recursive=False))
            npg = int(soup.find('ul', {"class": "pages"}).find_all("li",recursive=False)[numli-2].find("a")["data-value"])
            return (cpg < npg)
        except:
            return False


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
        run=True
        while run:
            soup = self.get_soup(httplib2.iri2uri(url + "#q[page]=" + str(pgid)))
            prods = soup.find_all('li', {"id": "list_offers"})
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
                        proddict['Product_name'] = prod.find("span", {"itemprop": "name"}).text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 108:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = config['site']+prod.find("span", {"class": "info-item"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 116:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        prodsoup = self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 126:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['Brand'] = prod.find("span", {"class": "brandName"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 121:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['Crossed_out_Price'] = float(prod.find("span", {"itemprop": "price"}).text.split("€")[0].replace(",",".").strip())
                        proddict['Price'] = float(prod.find("span", {"itemprop": "price"}).text.split("€")[1].replace(",",".").strip())
                    except Exception as e:
                        self.logger.error("Line 136:" + str(e))
                        try:
                            proddict['Price'] = float(prod.find("span", {"itemprop": "price"}).text.split("€")[0].replace(",",".").strip())
                            proddict['Crossed_out_Price'] = "None"
                        except Exception as e:
                            self.logger.error("Line 141:" + str(e))
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Availability'] = prodsoup.find("div",{"class":"block-infos-stock row"}).text.split("\n")[0].strip()
                    except Exception as e:
                        self.logger.error("Line 131:" + str(e))
                        proddict['Availability'] = "None"
                    try:
                        proddict['NumReviews'] = int(prodsoup.find("div",{"class":"rating"}).find("meta",{"itemprop":"ratingCount"})['content'])
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['NumReviews'] = 0
                    try:
                        proddict['Stars'] = int(prodsoup.find("div",{"class":"rating"}).find("meta",{"itemprop":"ratingValue"})['content'])
                    except Exception as e:
                        self.logger.error("Line 124:" + str(e))
                        proddict['Stars'] = 0.0
                    try:
                        proddict['Imagelink'] = "https:"+prod.find("img")['src']
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/")) - 1]
                    except Exception as e:
                        self.logger.error("Line 173:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    try:
                        proddict['EAN13'] = proddict['Imagefilename'].split("-")[-1:][0].split(".")[0]
                    except Exception as e:
                        self.logger.error("Line 148:" + str(e))
                        proddict['EAN13'] = "None"
                    try:
                        revs = prodsoup.find_all("li", {"id": re.compile("la_comment_front_bundle_comment_display.*")})
                        revlist = []
                        for rev in revs:
                            revdict = dict()
                            revdict['author'] = rev.find("span",{"class":"name"}).text.strip()
                            revdict['date'] = rev.find("em").text.strip().split(" ")[1]
                            revdict['revtext'] = rev.find("blockquote").text.strip()
                            revdict['rating'] = rev.find("span",{"class":"note_eval"}).text.split("/")[0]
                            revlist.append(revdict)
                        proddict["Reviews"] = revlist
                    except Exception as e:
                        self.logger.error("Line 148:" + str(e))
                        proddict["Reviews"] = "None"
                    try:
                        proddict['Discount_claim'] = prod.find("font",{"class":"_TOSPAN btn tag discount"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 158:" + str(e))
                        proddict['Discount_claim'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 100:" + str(e))
                    continue
            run = self.is_product(url + "#q[page]=" + str(pgid))
            pgid = pgid + 1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        # get mega category
        megacatlist = self.get_megacatgorylinks(soup)
        if (len(megacatlist)>0):
            for megacat in megacatlist:
                config['Mega-category'] = list(megacat.keys())[0]
                url = megacat[config['Mega-category']]
                soup = self.get_soup(url)
                catlist = self.get_catgorylinks(soup)
                if (len(catlist) > 0):
                    for cat in catlist:
                        config['Category'] = list(cat.keys())[0]
                        url = cat[config['Category']]
                        soup = self.get_soup(url)
                        seglist = self.get_allseg(soup)
                        if (len(seglist) > 0):
                            for seg in seglist:
                                config['segment'] = list(seg.keys())[0]
                                url = seg[config['segment']]
                                soup = self.get_soup(url)
                                subseglist = self.get_allsubseg(soup)
                                if (len(subseglist) > 0):
                                    for subseg in subseglist:
                                        config['Sub-segment'] = list(subseg.keys())[0]
                                        url = seg[config['Sub-segment']]
                                        self.get_proddata(url)
                                else:
                                    config['Sub-segment'] = "None"
                                    self.get_proddata(url)
                        else:
                            config['Sub-segment'] = "None"
                            config['segment'] = "None"
                            self.get_proddata(url)
                else:
                    config['Category'] = "None"
                    config['Sub-segment'] = "None"
                    config['segment'] = "None"
                    self.get_proddata(url)
        else:
            config['Mega-category'] = "None"
            config['Category'] = "None"
            config['Sub-segment'] = "None"
            config['segment'] = "None"
            self.get_proddata(url)
        pass


