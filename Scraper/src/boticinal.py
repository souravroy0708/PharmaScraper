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
import re
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class boticinal(threading.Thread):
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
        for item in soup.find("div",{"class":"section"}).find("ul").find_all("li",recursive=False):
            if (not(item.find("a", {"data-main":"1"})==None)):
                elem =item.find("a", {"data-main":"1"})
                if ("http" in elem['href']):
                    catdict=dict()
                    catdict[elem["data-name"].strip()]=elem['href']
                    catlist.append(catdict)
        return catlist
    
    def get_allseg(self,soup):
        seglist=[]
        for item in soup.find("div",{"class":"section"}).find("ul").find_all("li",recursive=False):
            try:
                if (item.find("a")["data-name"]==self.config['Category']):
                    for elem in item.find("div",{"class":"section"}).find("ul").find_all("li",recursive=False):
                        elem = elem.find("a")
                        try:
                            if ("http" in elem['href'] and not(elem['data-name']==self.config['Category'])):
                                segdict=dict()
                                segdict[elem["data-name"].strip()]=elem['href']
                                seglist.append(segdict)
                        except:
                            continue
                    break
                        
            except:
                continue
        return seglist
    
    def get_allsubseg(self,soup):
        subseglist=[]
        for item in soup.find("div",{"class":"section"}).find("ul").find_all("li"):
            try:
                if (item.find("a")["data-name"]==self.config['segment']):
                    for elem in item.find("div",{"class":"section"}).find("ul").find_all("li",recursive=False):
                        elem = elem.find("a")
                        try:
                            if ("http" in elem['href'] and not(elem['data-name']==self.config['segment'])):
                                subsegdict=dict()
                                subsegdict[elem["data-name"].strip()]=elem['href']
                                subseglist.append(subsegdict)    
                        except:
                            continue
                    break
                        
            except:
                continue
        return subseglist
    
    
    def is_product(self,url):
        soup = self.get_soup(httplib2.iri2uri(url))
        currpageid=url.split("=")[len(url.split("="))-1]
        try:
            cstr=soup.find('div', {"class":"pager"}).find("a")['href']
            newpageid=cstr.split("=")[len(cstr.split("="))-1]
            return(currpageid < newpageid)
        except:
            try:
                cstr=soup.find('div', {"class":"pager loaded"}).find("a")['href']
                newpageid=cstr.split("=")[len(cstr.split("="))-1]
                return(currpageid < newpageid)
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
        while (self.is_product(url+"?p="+str(pgid))):
            soup = self.get_soup(httplib2.iri2uri(url+"#/page-"+str(pgid)))
            prodslist=soup.find_all('ul', {"class":"products-grid list"})
            prods=[]
            for item in prodslist:
                for elem in item.find_all("li",{"class":"item"}):
                    if ('placeholder' not in elem['class']):
                        prods.append(elem)
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
                        proddict['Product_name']=prod["data-name"].strip()
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 149:"+str(e))
                        proddict['Product_name']="None" 
                    try:
                        proddict['Brand']=prod.find("strong").text
                    except Exception as e: 
                        self.logger.error("Line 155:"+str(e))
                        proddict['Brand'] = "None"
                    try:
                        proddict['urltoproduct']=prod.find("h2",{"class":"product-info"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 60:"+str(e))
                        proddict['urltoproduct']="None"  
                    try:
                        proddict['Price']=float(prod.find("p",{"class":"special-price"}).find("span",{"class":"price"}).text.strip().replace("\xa0€","").replace(",","."))
                        proddict['Crossed_out_Price']=float(prod.find("p",{"class":"old-price"}).find("span",{"class":"price"}).text.strip().replace("\xa0€","").replace(",","."))
                    except Exception as e:
                        try:
                            self.logger.error("Line 165:"+str(e))
                            proddict['Price']=float(prod.find("span",{"class":"price"}).text.strip().replace("\xa0€","").replace(",","."))
                            proddict['Crossed_out_Price']="None"
                        except Exception as e:
                            self.logger.error("Line 170:"+str(e))
                            proddict['Price']="None"  
                            proddict['Crossed_out_Price']="None"
                    try:
                        proddict['Availability']=prod.find('button',{"class":"replace"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 177:"+str(e))
                        proddict['Availability']= "None"
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e: 
                        self.logger.error("Line 182:"+str(e))
                        prodsoup = "None"
                    try:
                        proddict['EAN7']=prodsoup.find("div",{"class":"sku"}).find("strong").text.strip()
                    except Exception as e: 
                        self.logger.error("Line 187:"+str(e))
                        proddict['EAN7'] = "None"
                    
                    proddict['Imagelink'] = prod.find("img",{"data-lazyload":"1"})['data-src']
                    proddict['Imagefilename'] = proddict['Imagelink'] .split("/")[len(proddict['Imagelink'] .split("/"))-1]   
                    try:
                        proddict['Promotional_claim'] = prod.find("div",{"class":"ribbon product_discount"}).find("span").text.strip() 
                    except Exception as e: 
                        self.logger.error("Line 195:"+str(e))
                        proddict['Promotional_claim'] = "None"
                    try:       
                        proddict["NumReviews"]=prod.find("div",{"class":"ratings"}).find("div",{"class":"value"}).text.replace("(","").replace(")","").strip()
                    except Exception as e: 
                        self.logger.error("Line 200:"+str(e))
                        proddict["NumReviews"]=0    
                    try:
                        regex = re.compile('.*rating star-.*')
                        revs = prodsoup.find_all("div",{"class":"review"})
                        revlist=[]
                        for rev in revs:
                            revdict=dict()
                            revdict['author']=rev.find("div",{"class":"user"}).text
                            revdict['date']=rev.find("span",{"class":"date"}).text.replace("Commentaire posté le ","")
                            revdict['revhead']=rev.find("div",{"class":"title"}).text
                            revdict['revtext']=rev.find("div",{"class":"detail"}).text
                            revdict['rating']=int(rev.find("div",{"class":regex})["class"][1].replace("star-",""))/2
                            revlist.append(revdict)
                        proddict["Reviews"]=revlist
                    except Exception as e: 
                        self.logger.error("Line 205:"+str(e))
                        proddict["Reviews"]="None"
                    
                    db['scrapes'].insert_one(proddict)
                    nins=nins+1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.error("Line 87:url" + (url+"?p="+str(pgid)))
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

    
    

    
