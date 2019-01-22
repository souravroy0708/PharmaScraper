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
from bs4 import BeautifulSoup


# define product page extraction class
class lafayette_1(threading.Thread):
    def __init__(self,config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
     
        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler(config['template']+"_"+config["site"].replace("/","_").replace(".","_").replace(":","")+".log")
        logger_handler.setLevel(logging.DEBUG)
     
        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
     
        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)
     
        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')
    
    def get_soup(self,url):
        page = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(page)
        return(soup)
        
    def get_catgorylinks(self,soup):
        catlist=[]
        for item in soup.find_all("ul",{"id":"menu"})[0].find_all("li",{"class":"main"}):
            if (item.find("a") != None):
                if (self.config['Mega-category'].lower() in item.find("a")['href'].lower()):
                    catdict=dict()
                    catdict[item.find("a").text]=item.find("a")['href']
                    catlist.append(catdict)
        return catlist
    
    def get_allseg(self,soup):
        try:
            segdict=dict()
            for anchor in soup.find('div', {"class":"categories-listing"}).find_all("div",{"class":"titre_categorie"}):
                segdict[anchor.find("a").text.replace(".","").strip()] = dict()
                segdict[anchor.find("a").text.replace(".","").strip()]=anchor.find("a")["href"]
        except:
            segdict=dict()
        return segdict
    
    
    def is_product(self,url):
        print(url)
        try:
            soup = self.get_soup(url)
            numprods=soup.find("div",{"id":"paging-haut"}).find("div",{"class":"align-left"}).text.replace("\t","").replace("\n","")
            numprods = [int(s) for s in numprods.split() if s.isdigit()]
            return(numprods[1]<numprods[2])
        except:
            return (False)
    
    
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
        run = True
        while (run):
            soup = self.get_soup(url+"?page="+str(pgid)+"&sort=3a")
            prods=soup.find_all('div', {"class":"col-xs-6 col-sm-4 product_thumb_mobile"})
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
                        proddict['urltoproduct']=prod.find("div",{"class":"name"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 94:"+str(e))
                        proddict['urltoproduct']="None"
                    try:
                        proddict['Product_name']=prod.find("div",{"class":"name"}).find("a")['title']
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Product_name']="None"
                    try:
                        proddict["Brand"] = prod.find("div",{"class":"labo-produit"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 106:"+str(e))
                        proddict["Brand"] = "None"
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 111:"+str(e))
                    try:
                        proddict['Availability']=prodsoup.find('div',{"class":"stock-product"}).text
                    except Exception as e:
                        try:
                            proddict['Availability']=prodsoup.find('div',{"class":"horstock-product"}).text
                        except:
                            self.logger.error("Line 115:"+str(e))
                            proddict['Availability']= "None"
                    try:
                        formats=prodsoup.find_all('div',{"class":"references"})
                        proddict["EAN13"]="None"
                        proddict["Format"]="None"
                        for div in formats:
                            if ("Référence" in div.text):
                                proddict["EAN13"]=div.text.split(":")[1].strip()
                            if ("Contenance" in div.text):
                                proddict["Format"]=div.find("span").text.strip()
                    except Exception as e:
                        self.logger.error("Line 120:"+str(e))
                        proddict["EAN13"]="None"
                        proddict["Format"]="None"
                    try:
                        proddict['Price'] = float((prod.find("span",{"class":"bigprice"}).text + prod.find("span",{"class":"smallprice"}).text).replace(",",".").replace("€",""))
                    except Exception as e:
                        self.logger.error("Line 133:"+str(e))
                        proddict['Price'] = "None"
                    try:
                        proddict['Imagelink'] = prodsoup.find("div",{"class":"image col-xs-12 col-sm-4"}).find("a")['href']
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/"))-1]
                    except Exception as e:
                        self.logger.error("Line 138:"+str(e))
                        proddict["Imagelink"]="None"
                        proddict["Imagefilename"]="None"
                    try:
                        proddict['Feature'] = prodsoup.find("div",{"class":"image col-xs-12 col-sm-4"}).find("div",{"class":"position: absolute;top: 0px;left: 0px;"}).find("img")["src"]
                    except Exception as e:
                        self.logger.error("Line 145:"+str(e))
                        proddict['Feature'] = "None"
                    db['scrapes'].insert_one(proddict)
                    nins=nins+1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 87:" + str(e))
                    continue
            run = self.is_product(url+"?page="+str(pgid))
            pgid =pgid+1
        client.close()
        pass
    
    
    def run(self):
        config=self.config
        url = config['urls']
        soup = self.get_soup(url)
        #get segments
        try:
            catlist=self.get_catgorylinks(soup)
        except:
            catlist=[]
        if (len(catlist)>0):
            for cat in catlist:
                config['Category']=list(cat.keys())[0]
                url=cat[config['Category']]
                soup = self.get_soup(url)
                try:
                    allseg= self.get_allseg(soup)
                except:
                    allseg = dict()
                if (len(allseg)>0):
                    for seg in list(allseg.keys()):
                        url = allseg[seg]
                        config['segment']=seg
                        soup = self.get_soup(url)
                        try:
                            allsubseg= self.get_allseg(soup)
                        except:
                            allsubseg =  dict()
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