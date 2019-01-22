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
import json
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class aprium(threading.Thread):
    def __init__(self,config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
     
        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler("logs/"+config['template']+"_"+config["site"].replace("/","_").replace(".","_").replace(":","")+".log")
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
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text)
        return(soup)
        
    def get_catgorylinks(self,soup):
        catlist=[]
        for item in soup.find("div",{"class":"top_menu top-level tmmegamenu_item"}).find_all("a",{"class":"top-level-menu-li-a tmmegamenu_item"}):
            catdict=dict()
            catdict[item.text.strip()]=item['href']
            catlist.append(catdict)
        return catlist
    
    def get_allseg(self,soup):
        segdict=dict()
        for anchor in soup.find("div",{"id":"categTree"}).findAll('a'):
            segdict[anchor.text.strip()] = anchor['href']
        return segdict
    
    
    def is_product(self,url):
        soup = self.get_soup(httplib2.iri2uri(url))
        return(len(soup.find_all('div', {"class":"product-container"}))>0)
    
    
    def get_proddata(self,url):
        config=self.config
        pgid =0
        client = pymongo.MongoClient(config["mongolink"])
        db = client[str(config['db'])]
        nins=0
        self.logger.info("Mega-category:"+config['Mega-category'])
        self.logger.info("Category:"+config['Category'])
        self.logger.info("segment:"+config['segment'])
        self.logger.info("Sub-segment:"+config['Sub-segment'])
        while (self.is_product(url+"&res_sf="+str(pgid))):
            soup = self.get_soup(httplib2.iri2uri(url+"&res_sf="+str(pgid)))
            prods=soup.find_all('div', {"class":"product-container"})
            self.logger.info("#Found products:" + str(len(prods)))
            for prod in prods:
                try:
                    proddict=dict()
                    proddict['Source']=config['site']
                    proddict['Mega-category']=config['Mega-category']
                    proddict['Category']=config['Category']
                    proddict['segment']=config['segment']
                    proddict['Sub-segment']=config['Sub-segment']
                    proddict['template']=config['template']
                    try:
                        proddict['urltoproduct']=prod.find("p",{"class":"product-name"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 95:"+str(e))
                        proddict['urltoproduct']="None"                 
                    try:
                        tempdict=json.loads(prod.find("p",{"class":"product-name"}).find("a")['onclick']
                                        .replace("dataLayer.push(","")
                                        .replace(");","").replace("\n","")
                                        .replace("},","}").replace("'","\"").replace("  "," "))
                        proddict['Brand']=tempdict['ecommerce']['click']['products'][0]['brand']
                        proddict['Product_name']=tempdict['ecommerce']['click']['products'][0]['name'].split("-")[0].strip()
                        proddict['Format']=tempdict['ecommerce']['click']['products'][0]['name'].split("-")[1].strip()
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except:
                        try:
                            proddict['Brand']=prod.find("span",{"class":"list-name"}).text
                            proddict['Product_name']=prod.find("span",{"class":"list-desc"}).text.split("-")[0].strip()
                            proddict['Format']=prod.find("span",{"class":"list-desc"}).text.split("-")[1].strip()
                            if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                                continue
                        except Exception as e:
                            self.logger.error("Line 100:"+str(e))
                            proddict['Brand']="None"
                            proddict['Product_name']="None"
                            proddict['Format']="None"
                    try:
                        proddict['Price'] = float(prod.find("span",{"class":"price product-price"}).text.replace("€",".").strip())
                        proddict['Crossed_out_Price'] = float(prod.find("span",{"class":"old-price product-price"}).text.replace("€",".").strip())
                    except Exception as e:
                        self.logger.error("Line 122:"+str(e))
                        proddict['Price'] = float(prod.find("span",{"class":"price product-price"}).text.replace("€",".").strip())
                        proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Availability']=prod.find('button',{"type":"submit"})['id']
                    except Exception as e:
                        self.logger.error("Line 129:"+str(e))
                        proddict['Availability']= "None"

                    proddict['Imagelink'] = config["urls"] + prod.find("img",{"id":"imageProduit"})['src']
                    proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    try:
                        proddict['Promotional_claim'] = prod.find("span",{"class":"reduc-label"}).text 
                    except Exception as e: 
                        self.logger.error("Line 137:"+str(e))
                        proddict['Promotional_claim'] = "None"
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e: 
                        self.logger.error("Line 142:"+str(e))
                        prodsoup = "None"
                    try:
                        proddict['Discount_claim'] = prodsoup.findAll("div",{"id":"prix_degressifs"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 147:"+str(e))
                        proddict['Discount_claim'] = "None"
                    db[str(config['collection'])].insert_one(proddict)
                    nins=nins+1
                except Exception as e:
                    self.logger.error("Line 87:url"+(url+"&res_sf="+str(pgid)))
                    self.logger.error("Line 87:" + str(e))
                    continue
            pgid =pgid+12
        self.logger.info("#insertions:" + str(nins))
        client.close()
        pass
    
    
    def run(self):
        config=self.config
        url = config['urls']
        soup = self.get_soup(httplib2.iri2uri(url))
        #get segments
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
    
