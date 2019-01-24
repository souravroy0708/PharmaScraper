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
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class lafayette_2(threading.Thread):
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
        
    def get_catgorylinks(self,soup):
        catlist=[]
        regex = re.compile('li\d+')
        for item in soup.find_all("li",{"id":regex}):
            if ("http" in item.find("a",{"class":"drop"})['href']):
                catdict=dict()
                catdict[item.find("a",{"class":"drop"}).text.strip()]=item.find("a",{"class":"drop"})['href']
                catlist.append(catdict)
            else:
                catdict=dict()
                catdict[item.find("a",{"class":"drop"}).text.strip()]=self.config["site"] + item.find("a",{"class":"drop"})['href']
        return catlist
    
    def get_allseg(self,soup):
        segdict=dict()
        for anchor in soup.find('ul', {"class":"sub-categories-list"}).find_all("li"):
            segdict[anchor.find("a").text.replace(".","").strip()] = dict()
            segdict[anchor.find("a").text.replace(".","").strip()]=anchor.find("a")["href"]
        return segdict

    
    def is_product(self,url):
        print(url)
        try:
            soup = self.get_soup(url)
            numprods=soup.find("a",{"class":"paginationNext"})
            return(len(numprods)>0)
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
            regex = re.compile('vignetteProduits \d+')
            prods=soup.find("div",{"class":"col-xs-12 col-md-9 contenuCategories"}).find_all('div', {"class":regex})
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
                        proddict['urltoproduct']=prod.find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 94:"+str(e))
                        proddict['urltoproduct']="None"
                    try:
                        proddict['Product_name']=prod.find("a")['title']
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 99:"+str(e))
                        proddict['Product_name']="None"
                    try:
                        proddict["Brand"] = prod.find("div",{"class":"vignetteLaboProduit"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 106:"+str(e))
                        proddict["Brand"] = "None"
                    try:
                        proddict['Price'] = float((prod.find("div",{"class":"prixBase"}).text.replace("\t","").replace("\n","").replace("€","").replace(",",".").strip()))
                    except Exception as e:
                        self.logger.error("Line 133:"+str(e))
                        proddict['Price'] = "None"    
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 111:"+str(e))
                    try:
                        proddict['Availability']=prodsoup.find('div',{"class":"stockAvailableFicheProduit"}).text.strip()
                    except Exception as e:
                        self.logger.error("Line 115:"+str(e))
                        proddict['Availability']= "None"    
                    try:
                        proddict["EAN13"]=prodsoup.find('div',{"class":"referenceFicheProduit"}).text.split(":")[1].strip().replace("Contenance","")
                    except Exception as e:
                        self.logger.error("Line 115:"+str(e))
                        proddict["EAN13"]= "None" 
                    try:
                        proddict["Format"]=prodsoup.find('div',{"class":"referenceFicheProduit"}).find("span").text
                    except Exception as e:
                        self.logger.error("Line 115:"+str(e))
                        proddict["Format"]= "None" 
                    try:
                        proddict['Imagelink'] = prodsoup.find("div",{"class":"col-xs-12 col-sm-5 blocImageFicheProduit"}).find("a")['href']
                        proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/"))-1]
                    except Exception as e:
                        self.logger.error("Line 138:"+str(e))
                        proddict["Imagelink"]="None"
                        proddict["Imagefilename"]="None"
                    try:
                        proddict['Feature'] = prod.find("div",{"class":"bandeauNouveau"}).text.strip()
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
            catlist = []
        if (len(catlist)>0):
            for cat in catlist:
                config['Category']=list(cat.keys())[0]
                url=cat[config['Category']]
                soup = self.get_soup(url)
                try:
                    allseg= self.get_allseg(soup)
                except:
                    allseg = {}
                if (len(allseg)>0):
                    for seg in list(allseg.keys()):
                        url = allseg[seg]
                        config['segment']=seg
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
    
    
