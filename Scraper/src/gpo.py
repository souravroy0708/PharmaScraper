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
import platform
import os
import threading
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# define product page extraction class
class gpo(threading.Thread):
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
            #define chrome options
            chrome_options = Options()
            chrome_options.add_argument('--dns-prefetch-disable')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--lang=en-US')
            chrome_options.add_argument("headless")
            #chrome_options.add_argument('--headless')
            if (platform.system()=="Darwin"):
                driver = webdriver.Chrome(os.getcwd()+"/chromedrivers/chromedriver_mac", chrome_options=chrome_options)
            elif(platform.system()=="Linux"):
                driver = webdriver.Chrome(os.getcwd()+"/chromedrivers/chromedriver_linux", chrome_options=chrome_options)
            else:
                driver = webdriver.Chrome(os.getcwd()+"/chromedrivers/chromedriver.exe", chrome_options=chrome_options)
            driver.get(url)
            html = driver.page_source
            soup = BeautifulSoup(html)
        except:
            self.logger.error("Error in selenium in :" +  str(url))
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
        for item in soup.find("div",{"id":"block_top_menu"}).find("ul").find_all("li", recursive=False):
            if (not(item.find("ul")==None)):
                catdict=dict()
                catdict[item.find("a").text.strip()]=item.find("a")['href']
                catlist.append(catdict)
        return catlist
    
    def get_allseg(self,soup):
        seglist=[]
        for anchor in soup.find("div",{"id":"categories_block_left"}).find("ul").find_all('li', recursive=False):
             if (not(anchor.find("ul")==None)):
                segdict=dict()
                segdict[anchor.find("a").text.strip()]=anchor.find("a")['href']
                seglist.append(segdict)
        return seglist
    
    def get_allsubseg(self,soup):
        subseglist=[]
        for anchor in soup.find("div",{"id":"categories_block_left"}).find("ul").find_all('li', recursive=False):
            subsegdict=dict()
            subsegdict[anchor.find("a").text.strip()]=anchor.find("a")['href']
            subseglist.append(subsegdict)
        return subseglist
    
    
    def is_product(self,url):
        soup = self.get_soup(httplib2.iri2uri(url))
        self.logger.info(url)
        cstr=soup.find('div', {"class":"product-count"}).text.strip()
        end=int(cstr.split("sur")[1].replace(".","").strip())
        start=int(cstr.split("sur")[0].split("-")[1].strip())
        return(start < end)
    
    
    def get_proddata(self,url):
        config=self.config
        pgid =1
        client = pymongo.MongoClient(config["mongolink"])
        db = client['pharmascrape']
        nins=0
        self.logger.info("Mega-category:"+config['Mega-category'])
        self.logger.info("Category:"+config['Category'])
        self.logger.info("segment:"+config['segment'])
        self.logger.info("Sub-segment:"+config['Sub-segment'])
        while (self.is_product(url+"#/page-"+str(pgid))):
            soup = self.get_soup(httplib2.iri2uri(url+"#/page-"+str(pgid)))
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
                        proddict['Product_name']=prod.find("a",{"class":"product-name"}).text.strip()
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Product_name']="None"  
                    try:
                        proddict['urltoproduct']=prod.find("a",{"class":"product-name"})['href']
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['urltoproduct']="None"  
                    try:
                        proddict['Price']=float(prod.find("span",{"class":"price product-price"}).text.strip().replace("€","."))
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Price']="None"   
                    try:
                        proddict['Crossed_out_Price']=float(prod.find("span",{"class":"old-price product-price"}).text.strip().replace("€","."))
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Crossed_out_Price']="None"   
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e: 
                        self.logger.error("Line 139:"+str(e))
                        prodsoup = "None"
                    try:
                        proddict['Brand']=prodsoup.find("div",{"class":"product_reference marqueProduit"}).find("a").text.strip()
                    except Exception as e: 
                        self.logger.error("Line 139:"+str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['EAN13']=prodsoup.find("div",{"class":"product_reference refProduit"}).find("span").text.strip()
                    except Exception as e: 
                        self.logger.error("Line 139:"+str(e))
                        proddict['EAN13'] = "None"
                    try:
                        proddict['Availability']=prodsoup.find('div',{"id":"availability_statut"}).find("span").text.strip()
                    except Exception as e:
                        self.logger.error("Line 118:"+str(e))
                        proddict['Availability']= "None"
                    proddict['Imagelink'] = prod.find("img")['src']
                    proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]   
                    try:
                        proddict['Promotional_claim'] = prod.find("span",{"class":"product-price-reduction"}).text 
                    except Exception as e: 
                        self.logger.error("Line 129:"+str(e))
                        proddict['Promotional_claim'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins=nins+1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 79:" + str(e))
                    continue
            pgid =pgid+1
        client.close()
        pass
    
    
    def run(self):
        config=self.config
        url = config['urls']
        soup = self.get_soup(url)
        #get segments
        #get segments
        catlist=self.get_catgorylinks(soup)
        if (len(catlist)>0):
            for cat in catlist:
                config['Category']=list(cat.keys())[0]
                url=cat[config['Category']]
                soup = self.get_soup(httplib2.iri2uri(url))
                allseg= self.get_allseg(soup)
                if (len(allseg)>0):
                    for seg in allseg:
                        config['segment']=list(seg.keys())[0]
                        url=seg[config['segment']]
                        soup = self.get_soup(httplib2.iri2uri(url))
                        allsubseg=self.get_allsubseg(soup)
                        if (len(allsubseg)>0):
                            for subseg in allsubseg:
                                config['Sub-segment']=list(subseg.keys())[0]
                                url=subseg[config['Sub-segment']]
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
    
    
