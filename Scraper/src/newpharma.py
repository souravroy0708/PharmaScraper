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
class newpharma(threading.Thread):
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
        for item in soup.find_all("a", class_=lambda x: x == 'page-header-mainMenu__btn')[1:]:
            catdict=dict()
            catdict[item['title']]=item['href']
            catlist.append(catdict)
        return catlist
    
    def get_allseg(self,soup):
        segdict=dict()
        for anchor in soup.find_all("li",{"class":"l1"}):
            segdict[anchor.find("a").text.strip()] = self.config['site']+anchor.find("a")['href'].strip()
        return segdict
    
    def get_allsubseg(self,soup):
        subsegdict=dict()
        for anchor in soup.find_all("div",{"class":"menu-l3"}):
            subsegdict[anchor.find("a").text.strip()] = self.config['site']+anchor.find("a")['href'].strip()
        return subsegdict
    
    
    def is_product(self,url):
        soup = self.get_soup(httplib2.iri2uri(url))
        try:
            isproduct = len(soup.find_all("div",{"class":"product-container fr"}))>0
            return(isproduct)
        except:
            return(False)
    
    
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
        while self.is_product(httplib2.iri2uri(url+"?page="+str(pgid))):
            soup = self.get_soup(httplib2.iri2uri(url+"?page="+str(pgid)))
            prods=soup.find_all("div",{"class":"product-container fr"})
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
                        if ("indisponible" in prods[0].find("div",{"class":"add-to-cart"}).text.replace("\n","").strip()):
                            proddict['Availability']="None"
                        else:
                            proddict['Availability']="En stock"
                    except Exception as e:
                        self.logger.error("Line 104:"+str(e))
                        proddict['Availability']="None" 
                    try:
                        proddict['NumReviews']=int(prod.find("span",{"class":"rating-text"}).find("a").text.replace("avis","").strip())
                    except Exception as e:
                        self.logger.error("Line 112:"+str(e))
                        proddict['NumReviews']=0
                    try:
                        proddict['Brand']=prod.find_all("a")[1]['href'].split("/")[1]
                    except Exception as e:
                        self.logger.error("Line 117:"+str(e))
                        proddict['Brand']="None" 
                    try:
                        proddict['Product_name']=prod.find_all("a")[1].text
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 122:"+str(e))
                        proddict['Product_name']="None"      
                    try:
                        proddict['Price'] = float(prod.find("div",{"class":"sale-price"}).text.replace("€","").replace("\n","").replace(",",".").strip())
                        proddict['Crossed_out_Price'] =float(prod.find("div",{"class":"original-price"}).text.replace("€","").replace("\n","").replace(",",".").strip())
                    except Exception as e:
                        try:
                            self.logger.error("Line 127:"+str(e))
                            proddict['Price'] = float(prod.find("div",{"class":"sale-price"}).text.replace("€","").replace("\n","").replace(",",".").strip())
                            proddict['Crossed_out_Price'] ="None"  
                        except Exception as e:
                            self.logger.error("Line 132:"+str(e))
                            proddict['Price'] = "None"  
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Promotional_claim'] = len(prod.find_all("div",{"class":"badge promo"}))>0
                    except Exception as e: 
                        self.logger.error("Line 139:"+str(e))
                        proddict['Promotional_claim'] = "None"
                        
                    try:
                        proddict['Stars'] = prod.find("span",{"class":"star-ratings-sprite"}).find("span")['style'].replace("width:","")
                    except Exception as e: 
                        self.logger.error("Line 145:"+str(e))
                        proddict['Stars'] = "None"
                    try:
                        proddict['Discount_claim'] = prod.find("span",{"class":"discount-percent"}).text 
                    except Exception as e: 
                        self.logger.error("Line 150:"+str(e))
                        proddict['Discount_claim'] = "None"
                    proddict['Imagelink'] = prod.find("img")['src']
                    proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]
                    try:
                        proddict['reveiewlink']=config['site']+prod.find("span",{"class":"rating-text"}).find("a")['href']
                        revsoup=self.get_soup(httplib2.iri2uri(proddict['reveiewlink']))
                        try:
                            revs = revsoup.find_all("div",{"itemprop":"review"})
                            revlist=[]
                            for rev in revs:
                                revdict=dict()
                                revdict['author']=rev.find("span",{"itemprop":"name"}).text
                                revdict['date']=rev.find("span",{"itemprop":"datePublished"}).text
                                revdict['revtext']=rev.find("div",{"itemprop":"reviewBody"}).text
                                revdict['rating']=rev.find("div",{"itemprop":"ratingValue"})['content']
                                revlist.append(revdict)
                            proddict["Reviews"]=revlist
                        except Exception as e: 
                            self.logger.error("Line 157:"+str(e))
                            proddict["Reviews"]="None"    
                    except Exception as e:
                        self.logger.error("Line 157:"+str(e))
                        proddict["Reviews"]="None"
                    try:
                        proddict['urltoproduct']=config['site']+prod.find_all("a")[1]['href']
                    except Exception as e:
                        self.logger.error("Line 96:"+str(e))
                        proddict['urltoproduct']="None"                 
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
    
    