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
class mesoigner(threading.Thread):
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
        for item in soup.find_all("ul",{"class":"main-menu-lvl1"})[1]:
            catdict=dict()
            catdict[item.find("span",{"class":"main-menu-title"}).text]=self.config['site']+item.find("a")['href']
            catlist.append(catdict)
        return catlist
    
    def get_allseg(self,soup):
        catdata = soup.find_all("ul",{"class":"main-menu-lvl1"})[1]
        for item in catdata:
            if (item.find("span",{"class":"main-menu-title"}).text == self.config['Category']):
                break
        seglist=[]
        for elem in item.find("ul",{"class":"main-menu-lvl2"}).find_all("ul",{"class":"main-menu-lvl3"}):
            elem.decompose()
        for elem in item.find("ul",{"class":"main-menu-lvl2"}).find_all("li"):
            segdict=dict()
            segdict[elem.find("a").text]=self.config['site']+elem.find("a")['href']
            seglist.append(segdict)
        return seglist
    
    def get_allsubseg(self,soup):
        catdata = soup.find_all("ul",{"class":"main-menu-lvl1"})[1]
        for item in catdata:
            if (item.find("span",{"class":"main-menu-title"}).text == self.config['Category']):
                break
        for elem in item:
            if (elem.text == self.config['segment']):
                break
        subseglist=[]
        for item in elem.find("ul",{"class":"main-menu-lvl3"}).find_all("li"):
            subsegdict=dict()
            subsegdict[item.find("a").text]=self.config['site']+item.find("a")['href']
            subseglist.append(subsegdict)
        return subseglist
    
    
    def is_product(self,url):
        print(url)
        try:
            soup = self.get_soup(url)
            numprods=soup.find("ul",{"class":"pagination"}).find_all("li",{"class":"page-item d-none d-md-block"})
            numprods = int(numprods[len(numprods)-1].text.replace("\n","").strip())
            currpg = int(url.split("/")[len(url.split("/"))-1])
            return(currpg<numprods)
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
            soup = self.get_soup(url+"/"+str(pgid))
            prods=soup.find_all('div', {"class":"product mb-5"})
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
                        proddict['urltoproduct']=config['site']+prod.find("h2",{"class":"product-title"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 94:"+str(e))
                        proddict['urltoproduct']="None"
                    try:
                        proddict['Product_name']=prod.find("h2",{"class":"product-title"}).find("a")['title']
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Product_name']="None"
                    try:
                        proddict['Price'] = float(prod.find("span",{"class":"price"}).text.replace("\n","").replace("TTC","").replace("\xa0€","").replace(",",".").strip())
                    except Exception as e:
                        self.logger.error("Line 133:"+str(e))
                        proddict['Price'] = "None"
                    try:
                        proddict["Brand"] = prod.find("h3",{"class":"product-subtitle"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 106:"+str(e))
                        proddict["Brand"] = "None"
                    try:
                        proddict['Crossed_out_Price'] = float(prod.find("s",{"class":"text-promo"}).text.replace("\n","").replace("TTC","").replace("\xa0€","").replace(",",".").strip())
                    except Exception as e:
                        self.logger.error("Line 133:"+str(e))
                        proddict['Crossed_out_Price'] = "None" 
                    try:
                        proddict["Promotional_claim"] = prod.find("p",{"class":"bg-promo p-2 mb-2 text-center text-promo text-uppercase"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 146:"+str(e))
                        proddict["Promotional_claim"] = "None"
                    try:
                        proddict['Imagelink'] = prod.find("img")['src']
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/"))-1]
                    except Exception as e:
                        self.logger.error("Line 138:"+str(e))
                        proddict["Imagelink"]="None"
                        proddict["Imagefilename"]="None"
                    db['scrapes'].insert_one(proddict)
                    nins=nins+1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 87:" + str(e))
                    continue
            run = self.is_product(url+"/"+str(pgid))
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
                    allseg=[]
                if (len(allseg)>0):
                    for seg in allseg:
                        config['segment']= list(seg.keys())[0]
                        url =seg[config['segment']]
                        soup = self.get_soup(url)
                        try:
                            allsubseg= self.get_allsubseg(soup)
                        except:
                            allsubseg=[]
                        if (len(allsubseg)>0):
                            for subseg in allsubseg:
                                config['Sub-segment']= list(subseg.keys())[0]
                                url =subseg[config['Sub-segment']]
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

    
