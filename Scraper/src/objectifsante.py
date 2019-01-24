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
import re
from bs4 import BeautifulSoup

# define product page extraction class
class objectifsante(threading.Thread):
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

    def get_soup(self,url):
        try:
            page = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(page)
        except:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text)
        return (soup)

    def get_menu(self,soup):
        menudict=dict()
        menudict['pharmacie']=dict()
        menudict['parapharmacie'] = dict()
        for item in soup.find_all("li"):
            try:
                if item.find("a")!=None and item.find("a")["id"]=="pharmacieDropdown":
                    for anchor in item.find("ul",{"class":"list-unstyled"}).find_all("li",recursive=False):
                        menudict['pharmacie'][anchor.find("a").text.strip()]=dict()
            except:
                continue
        for item in soup.find_all("li"):
            try:
                if item.find("a")!=None and item.find("a")["id"]=="parapharmacieDropdown":
                    for anchor in item.find("ul",{"class":"list-unstyled"}).find_all("li",recursive=False):
                        menudict['parapharmacie'][anchor.find("a").text.strip()]=dict()
            except:
                continue
        for item in soup.find_all("li"):
            try:
                if item.find("a")!=None and item.find("a")["id"]=="pharmacieDropdown":
                    for anchor in item.find_all("div",{"id":re.compile(r'(parent-)')}):
                        for elem in anchor.find_all("div",{"class":"child-menu"}):
                            menudict['pharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()]=dict()
            except:
                continue
        for item in soup.find_all("li"):
            try:
                if item.find("a")!=None and item.find("a")["id"]=="parapharmacieDropdown":
                    for anchor in item.find_all("div",{"id":re.compile(r'(parent-)')}):
                        for elem in anchor.find_all("div",{"class":"child-menu"}):
                            menudict['parapharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()]=dict()
            except:
                continue
        for item in soup.find_all("li"):
            try:
                if item.find("a")!=None and item.find("a")["id"]=="pharmacieDropdown":
                    for anchor in item.find_all("div",{"id":re.compile(r'(parent-)')}):
                        for elem in anchor.find_all("div",{"class":"child-menu"}):
                            if (len(elem.find_all("li"))>0):
                                for subseg in elem.find("ul",{"class":"child-child-menu list-unstyled"}).find_all("li",recursive=False):
                                    menudict['pharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][
                                        subseg.find("a").text.strip()]=dict()
                                    menudict['pharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][subseg.find("a").text.strip()]['url']=config['site']+subseg.find("a")['href']
                            else:
                                menudict['pharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][
                                    subseg.find("a").text.strip()]=dict()
                                menudict['pharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][
                                    subseg.find("a").text.strip()]['url']=config['site']+elem.find("a")['href']
            except:
                continue
        for item in soup.find_all("li"):
            try:
                if item.find("a")!=None and item.find("a")["id"]=="parapharmacieDropdown":
                    for anchor in item.find_all("div",{"id":re.compile(r'(parent-)')}):
                        for elem in anchor.find_all("div",{"class":"child-menu"}):
                            if (len(elem.find_all("li"))>0):
                                for subseg in elem.find("ul",{"class":"child-child-menu list-unstyled"}).find_all("li",recursive=False):
                                    menudict['parapharmacie'][anchor.find("h3").text.strip()][
                                        elem.find("a").text.strip()][subseg.find("a").text.strip()]=dict()
                                    menudict['parapharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][subseg.find("a").text.strip()]['url']=config['site']+subseg.find("a")['href']
                            else:
                                menudict['parapharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][
                                    subseg.find("a").text.strip()]=dict()
                                menudict['parapharmacie'][anchor.find("h3").text.strip()][elem.find("a").text.strip()][
                                    subseg.find("a").text.strip()]['url']=config['site']+elem.find("a")['href']
            except:
                continue

        return menudict


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
            soup = self.get_soup(url + "?page=" + str(pgid))
            prods = soup.find_all('div', {"class": "col-6 col-sm-4 col-lg-3"})
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
                        proddict['Product_name'] = prod.find("h5",{"class":"card-title"}).find("a").text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 153:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = self.config["site"] + prod.find("h5",{"class":"card-title"}).find("a")['href']
                    except Exception as e:
                        proddict['urltoproduct'] = "None"
                    try:
                        proddict['Brand'] = prod.find("div",{"class":"font-size-1"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 134:" + str(e))
                        proddict['Brand'] = "None"
                    try:
                        prodsoup = self.get_soup(httplib2.iri2uri(proddict['urltoproduct']))
                    except:
                        self.logger.error("Line 114:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['Price'] = float(prod.find("span", {"id": "product-price"}).text.replace("\n","").replace("\xa0€", "").replace(",", ".").strip())
                    except Exception as e:
                        self.logger.error("Line 118:" + str(e))
                        proddict['Price'] = "None"
                    try:
                        proddict['EAN13'] = int(prodsoup.find("small",{"class":"text-black-50"}).text.split(":")[1].strip())
                    except Exception as e:
                        self.logger.error("Line 118:" + str(e))
                        proddict['EAN13'] = "None"
                    try:
                        proddict['Promotional_Claim'] = prod.find("div", {"class": "promotion-block bg-primary text-white position-absolute font-size-1"}).find("strong").text.strip()
                    except Exception as e:
                        self.logger.error("Line 129:" + str(e))
                        proddict['Promotional_Claim'] = "None"
                    try:
                        proddict['Imagelink'] = prod.find("a",{"class":"card-img-top"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    except Exception as e:
                        self.logger.error("Line 129:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins = nins + 1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 79:" + str(e))
                    continue
            if (len(soup.find_all("div",{"class":"item next disabled"}))>0):
                run=False
            pgid = pgid + 1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        menudict = self.get_menu(soup)
        for key1 in list(menudict.keys()):
            config['Mega-category'] =key1
            megadict = menudict[key1]
            if ('url' in list(megadict.keys())):
                config['Category'] = "None"
                config['segment'] = "None"
                config['Sub-segment'] = "None"
                self.get_proddata(megadict['url'])
            else:
                for key2 in list(megadict.keys()):
                    config['Category'] = key2
                    catdict = megadict[key2]
                    if ('url' in list(catdict.keys())):
                        config['segment'] = "None"
                        config['Sub-segment'] = "None"
                        self.get_proddata(catdict['url'])
                    else:
                        for key3 in list(catdict.keys()):
                            config['segment'] = key3
                            segdict = catdict[key3]
                            if ('url' in list(segdict.keys())):
                                config['Sub-segment'] = "None"
                                self.get_proddata(segdict['url'])
                            else:
                                for key4 in list(segdict.keys()):
                                    config['Sub-segment'] = key4
                                    subsegdict = segdict[key4]
                                    if ('url' in list(subsegdict.keys())):
                                        self.get_proddata(subsegdict['url'])
        pass


