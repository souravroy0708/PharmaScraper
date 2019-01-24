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
class cocooncenter(threading.Thread):
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
        page = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(page)
        return(soup)
        
    def get_megacatgorylinks(self,soup):
        try:
            megacatlist=[]
            for item in soup.find_all("li",{"class":" menu_top_vert_orange"}):      
                if (item.find("a") != None):
                    megacatdict=dict()
                    megacatdict[item.find("a").text]=self.config["site"]+item.find("a")['href']
                    megacatlist.append(megacatdict)
        except:
            megacatlist=[]
        return megacatlist
        
    def get_catgorylinks(self,soup):
        try:
            catlist=[]
            for item in soup.find("ul",{"class":"nav_left"}).find_all("li"):
                if (item.find("a") != None):
                    catdict=dict()
                    catdict[item.find("a").text]=self.config["site"]+item.find("a")['href']
                    catlist.append(catdict) 
        except:
            catlist=[]
        return catlist
    
    def get_allseg(self,soup):
        try:
            seglist=[]
            for item in soup.find('li', {"class":"li_selected "}).find_all("li"):
                if (item.find("a") != None):
                    segdict=dict()
                    segdict[item.find("a").text]=self.config["site"]+item.find("a")['href']
                    seglist.append(segdict) 
        except:
            seglist=[]
        return seglist
    
    
    def is_product(self,url):
        print(url)
        try:
            soup = self.get_soup(url)
            numprods=soup.find("span",{"id":"total_produit_listing"}).text
            numprods=numprods.split("-")[1]
            numprods = [int(s) for s in numprods.split() if s.isdigit()]
            return(numprods[0]<numprods[1])
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
            soup = self.get_soup(url+"?page="+str(pgid))
            prods=soup.find_all('li', {"class":"fiche_min fiche_min_categorie"})
            self.logger.info("#Found products:" + str(len(prods)))
            for prod in prods:
                try:
                    proddict=dict()
                    proddict['Source']=config['site']   
                    proddict['Mega-category']=config['Mega-category']
                    proddict['Category']=config['Category']
                    proddict['segment']=config['segment']
                    proddict['Sub-segment']=config['Sub-segment']
                    try:
                        proddict['Availability']=prod.find('span',{"class":"fmc_txt_article_disponible"}).text
                    except Exception as e:
                        self.logger.error("Line 96:"+str(e))
                        proddict['Availability']= "None"
                    try:
                        proddict['Price'] = float(prod.find("span",{"class":"texte_current_prix"}).text.replace("\n","").replace("\xa0€","").replace(",",".").strip())
                        proddict['Crossed_out_Price'] = float(prod.find("span",{"class":"texte_ancien_prix"}).text.replace("\n","").replace("\xa0€","").replace(",",".").strip())
                    except Exception as e:
                        try:
                            self.logger.error("Line 102:"+str(e))
                            proddict['Price'] = float(prod.find("span",{"class":"texte_current_prix"}).text.replace("\n","").replace("\xa0€","").replace(",",".").strip())
                            proddict['Crossed_out_Price'] = "None"
                        except:
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Product_name']=prod.find("h3",{"class":"bloc_nom"}).text
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 112:"+str(e))
                        proddict['Product_name']="None"
                    try:
                        proddict['urltoproduct']=config["site"] + prod.find("h3",{"class":"bloc_nom"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 101:"+str(e))
                        proddict['urltoproduct']="None"      
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 106:"+str(e))
                    try:
                        proddict["Brand"] = prodsoup.find("span",{"itemprop":"brand"}).text
                    except Exception as e: 
                        self.logger.error("Line 126:"+str(e))
                        proddict["Brand"] = "None"
                    try:
                        proddict["Format"] = prodsoup.find("div",{"class":"type_packaging_fiche_produit"}).text.replace("\t","").replace("\r","").replace("\n","")
                    except Exception as e: 
                        self.logger.error("Line 131:"+str(e))
                        proddict["Format"] = "None"
                    try:
                        proddict["EAN13"] = prodsoup.find("div",{"class":"fiche_produit_ean_only "}).text.replace("\t","").replace("\r","").replace("\n","").replace("EAN","").replace(":","").strip()
                    except Exception as e: 
                        self.logger.error("Line 131:"+str(e))
                        proddict["EAN13"] = "None"
                    try:
                        proddict["Loyalty"] = prodsoup.find("div",{"id":"bloc_fidelite_cumulee"}).text.replace("\t","").replace("\r","").replace("\n","").replace("\xa0","").strip()
                    except Exception as e: 
                        self.logger.error("Line 141:"+str(e))
                        proddict["Loyalty"] = "None" 
                    try:
                        proddict["Promotional_claim"] = prodsoup.find("span",{"class":"container_vignette_promotion_notxt"})=="None"
                    except Exception as e: 
                        self.logger.error("Line 146:"+str(e))
                        proddict["Promotional_claim"] = "None"    
                    try:
                        proddict["Discount_claim"] = prodsoup.find("div",{"id":"texte_ancien_prix"}).text.replace("\t","").replace("\r","").replace("\n","").split("(")[1].split(")")[0]
                    except Exception as e: 
                        self.logger.error("Line 141:"+str(e))
                        proddict["Discount_claim"] = "None" 
                    try:
                        proddict["Imagelink"] = "https://"+prodsoup.find("div",{"id":"div_image"}).find("a")["href"]
                        proddict['Imagefilename'] = proddict["Imagelink"].split("/")[len(proddict["Imagelink"].split("/"))-1]
                    except Exception as e: 
                        self.logger.error("Line 156:"+str(e))
                        proddict["Imagelink"] = "None" 
                        proddict["Imagefilename"] = "None" 
                    db['scrapes'].insert_one(proddict)
                    nins=nins+1
                    self.logger.info("#insertions:" + str(nins))
                except Exception as e:
                    self.logger.info("soup:" + str(prod))
                    self.logger.error("Line 88:" + str(e))
                    continue
            run = self.is_product(url+"?page="+str(pgid))
            pgid =pgid+1
        client.close()
        pass
    
    
    def run(self):
        config=self.config
        url = config['urls']
        soup = self.get_soup(url)
        #get megacategory
        megacatlist = self.get_megacatgorylinks(soup)
        for megacat in megacatlist:
            config['Mega-category']=list(megacat.keys())[0]
            url = megacat[config['Mega-category']]
            soup = self.get_soup(url)
            catlist=self.get_catgorylinks(soup)
            if (len(catlist)>0):
                for cat in catlist:
                    config['Category']=list(cat.keys())[0]
                    url = cat[config['Category']]
                    soup = self.get_soup(url)
                    seglist =self.get_allseg(soup)
                    if (len(seglist)>0):
                        for seg in seglist:
                            config['segment']=list(seg.keys())[0]
                            url = seg[config['segment']]
                            soup = self.get_soup(url)
                            subseglist = self.get_allseg(soup)
                            if (len(subseglist)>0):
                                for subseg in subseglist:
                                    config['Sub-segment']=list(subseg.keys())[0]
                                    url = subseg[config['Sub-segment']]
                                    self.get_proddata(url)  
                            else:
                                self.get_proddata(url)  
                    else:
                        self.get_proddata(url)        
            else:
                self.get_proddata(url) 
        pass
    