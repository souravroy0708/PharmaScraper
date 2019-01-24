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
from bs4 import BeautifulSoup


# define product page extraction class
class lasante(threading.Thread):
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

    def get_menu(self, soup):
        structure = dict()
        cats = soup.find("ul",{"class":"navigation__list js-level js-top-level"}).find_all("li",recursive=False)
        #creating category and megacategory
        structure["Médicaments"]=dict()
        structure["Parapharmacie"] = dict()
        medicament = cats[0]
        parapharmacie = cats[1:]
        medicamentlist=dict()
        for elem in medicament.find_all("a"):
            medicamentlist[elem.text.strip()]=self.config['site']+elem['href']
        structure["Médicaments"] = medicamentlist
        for elem in parapharmacie:
            tempdict=dict()
            for item in elem.find_all("a"):
                tempdict[item.text.strip()] = self.config['site']+item['href']
            structure["Parapharmacie"][elem.find("a").text.strip()]=tempdict
        return structure



    def get_proddata(self, url):
        config = self.config
        pgid = 1
        client = pymongo.MongoClient(config["mongolink"])
        db = client['pharmascrape']
        nins = 0
        run = True
        finalpg = 1
        self.logger.info("Mega-category:" + config['Mega-category'])
        self.logger.info("Category:" + config['Category'])
        self.logger.info("segment:" + config['segment'])
        self.logger.info("Sub-segment:" + config['Sub-segment'])
        while run:
            soup = self.get_soup(httplib2.iri2uri(url))
            if finalpg == 1:
                try:
                    finalpg = int(soup.find("p",{"class":"prodlist__pagination--current"}).find_all("span")[1].text)
                except:
                    finalpg = 0
            prods = soup.find("ul", {"class": "prodlist__prods"}).find_all("li",recursive=False)
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
                        proddict['Product_name'] = prod.find("p", {"class": "related__desc "}).text.strip()
                        if (db['scrapes'].find(
                                {"Source": config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 102:" + str(e))
                        proddict['Product_name'] = "None"
                    try:
                        proddict['urltoproduct'] = config['site']+prod.find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 109:" + str(e))
                        proddict['urltoproduct'] = "None"
                    try:
                        prodsoup = self.get_soup(httplib2.iri2uri(proddict['urltoproduct']))
                    except:
                        self.logger.error("Line 114:" + str(e))
                        prodsoup = "None"
                    try:
                        proddict['Format'] = prodsoup.find("span",{"class":"produit__capacity"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 109:" + str(e))
                        proddict['Format'] = "None"
                    try:
                        proddict['Price'] = float(prodsoup.find("span", {"class": "produit__price produit__price--promo"}).text.replace("€","").strip())
                        proddict['Crossed_out_Price'] = float(prodsoup.find("span", {"class": "produit__price produit__price--old"}).text.replace("€","").strip())
                    except Exception as e:
                        try:
                            self.logger.error("Line 134:" + str(e))
                            proddict['Price'] = float(prodsoup.find("span", {"class": "produit__price"}).text.replace("€","").strip())
                            proddict['Crossed_out_Price'] = "None"
                        except Exception as e:
                            self.logger.error("Line 139:" + str(e))
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['EAN13'] = int(prodsoup.find("span", {"class": "produit__reference"}).text.replace("Référence : ","").strip())
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['EAN13 '] = 0
                    try:
                        proddict["Discount_claim"] = proddict['Crossed_out_Price'] - proddict['Price']
                    except Exception as e:
                        self.logger.error("Line 151:" + str(e))
                        proddict["Discount_claim"] = "None"
                    try:
                        proddict['NumReviews'] = int(prodsoup.find("a", {"href": "#avis"}).text.replace(" avis","").strip())
                    except Exception as e:
                        self.logger.error("Line 119:" + str(e))
                        proddict['NumReviews'] = 0
                    try:
                        proddict['Stars'] = prodsoup.find("a", {"href": "#avis"}).find("span")["class"][1].replace("icon__stars--","")
                    except Exception as e:
                        self.logger.error("Line 124:" + str(e))
                    try:
                        proddict['Imagelink'] = prodsoup.find("div",{"class":"produit__view"}).find("img")["src"]
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[
                            len(proddict['Imagelink'].split("/")) - 1]
                    except:
                        self.logger.error("Line 161:" + str(e))
                        proddict['Imagelink'] = "None"
                        proddict['Imagefilename'] = "None"
                    try:
                        try:
                            revlist = []
                            for rev in prodsoup.find_all("div", {"class": "produit__rate"}):
                                revdict = dict()
                                revdict['author'] = rev.find("p",{"class":"rating__author"}).text.strip()
                                revdict['date'] = rev.find("p",{"class":"rating__date"}).text.strip()
                                revdict['revtext'] = rev.find("p",{"class":"rating__message"}).text.strip()
                                revdict['rating'] = rev.find("p",{"class":"rating__numb"}).text.strip().split("/")[0]
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
            if (pgid == finalpg):
                run = False
            pgid = pgid +1
        client.close()
        pass

    def run(self):
        config = self.config
        url = config['urls']
        soup = self.get_soup(url)
        try:
            catlist = self.get_menu(soup)
        except:
            catlist = []
        medicament = catlist['Médicaments']
        parapharmacie = catlist['Parapharmacie']
        for item in list(medicament.keys()):
            url = medicament[item]
            if ('/' not in url or url.count('/') <= 5):
                continue
            else:
                splits = url.split("/")
                splits = [x for x in splits if x]
                config['Sub-segment'] = splits[len(splits)-1]
                config['segment'] = splits[len(splits)-2]
                config['Category'] = splits[len(splits) - 3]
                config['Mega-category'] = 'Médicaments'
                try:
                    self.get_proddata(url)
                except:
                    continue
        for item in list(parapharmacie.keys()):
            currdict = parapharmacie[item]
            for elem in list(currdict.keys()):
                url = currdict[elem]
                if ('/' not in url or url.count('/') <= 5):
                    continue
                else:
                    splits = url.split("/")
                    splits = [x for x in splits if x]
                    config['Sub-segment'] = splits[len(splits)-1].split(".")[0]
                    config['segment'] = splits[len(splits)-2]
                    config['Category'] = splits[len(splits) - 3]
                    config['Mega-category'] = 'Parapharmacie'
                    try:
                        self.get_proddata(url)
                    except:
                        continue
        pass
