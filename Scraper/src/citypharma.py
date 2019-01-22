    #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import urllib
import pymongo
import logging
import re
import requests
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class citypharma(threading.Thread):
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
        try:
            page = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(page)
        except:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text)
        return(soup)
        
    
    def get_allseg(self,soup):
        segdict=dict()
        regex = re.compile('level1 nav.*')
        for anchor in soup.find_all('li', {"class":regex}):
            segdict[anchor.find("a").text.replace(".","").strip()] = dict()
            segdict[anchor.find("a").text.replace(".","").strip()]=anchor.find("a")["href"]
        return segdict
    
    def get_allsubseg(self,soup):
        subsegdict=dict()
        for anchor in soup.find("div",{"class":"col-left sidebar"}).find("div").find("div",{"class":"block-content"}).find("dl").find("dd").find("ol").find_all('li'):
            subsegdict[anchor.find("a").text.strip()] = dict()
            subsegdict[anchor.find("a").text.strip()] = anchor.find("a")["href"]
        return subsegdict
    
    def is_product(self,url):
        self.logger.info(url)
        try:
            soup = self.get_soup(url)
            isnext=not(soup.find("a",{"class":"next i-next"})==None)
            self.logger.info(str(isnext))
            return(isnext)
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
            soup = self.get_soup(url+"&p="+str(pgid))
            regex = re.compile('item.*')
            prods=soup.find_all("li",{"class":regex})
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
                        proddict['Product_name']=prod.find("h2",{"class":"product-name"}).find("a").text.strip()
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Product_name']="None"
                    try:
                        proddict["Format"]=prod.find("h2",{"class":"product-name"}).find('div',{"class":"number"}).text
                    except Exception as e:
                        self.logger.error("Line 115:"+str(e))
                        proddict["Format"]= "None" 
                    try:
                        proddict['urltoproduct']=prod.find("h2",{"class":"product-name"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 94:"+str(e))
                        proddict['urltoproduct']="None"
                    try:
                        proddict['Price'] = float((prod.find("span",{"class":"price"}).text.replace("\xa0€","").replace(",",".").strip()))
                    except Exception as e:
                        self.logger.error("Line 133:"+str(e))
                        proddict['Price'] = "None"    
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
            run = self.is_product(url+"&p="+str(pgid))
            pgid =pgid+1
        client.close()
        pass
    
    
    def run(self):
        config=self.config
        url = config['urls']
        soup = self.get_soup(url)
        #get segments
        allseg= self.get_allseg(soup)
        if (len(allseg)>0):
            for seg in list(allseg.keys()):
                url = allseg[seg]
                soup = self.get_soup(url)
                config['segment']=seg
                allsubseg=self.get_allsubseg(soup)
                if (len(allsubseg)>0):
                    for subseg in list(allsubseg.keys()):
                        config['Sub-segment']=subseg
                        url=allsubseg[subseg]
                        self.get_proddata(url)
                else:
                    config['Sub-segment']="None"
                    self.get_proddata(url)
        else:
            config['segment']="None"
            config['Sub-segment']="None"
            self.get_proddata(url)  
        pass
