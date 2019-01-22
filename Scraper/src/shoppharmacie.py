#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 09:28:45 2018

@author: abhishekray
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhishekray
"""
import urllib
import pymongo
import re
import logging
import threading
from bs4 import BeautifulSoup


# define product page extraction class
class shoppharmacie(threading.Thread):
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
        for item in soup.find_all("a",{"class":"category-link"}):
            catdict=dict()
            catdict[item.text]=item['href']
            catlist.append(catdict)    
        return catlist[9:]
    
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
            soup = self.get_soup(url+"?pageNumber="+str(pgid))
            prods=soup.find_all('div', {"class":"result-list-entry"})
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
                        regex = re.compile('.*stock-status.*')
                        proddict['Availability']=prod.find('div',{"class":regex}).text.replace("\n","").strip()
                    except Exception as e:
                        self.logger.error("Line 96:"+str(e))
                        proddict['Availability']= "None"
                    try:
                        proddict['Stars']=float(prod.find('input',{"name":"score"})["value"])
                    except Exception as e:
                        self.logger.error("Line 96:"+str(e))
                        proddict['Stars']= "None" 
                    try:
                        proddict['NumReviews']=float(prod.find('span',{"class":"total-ratings"}).text.replace("(","").replace(")",""))
                    except Exception as e:
                        self.logger.error("Line 96:"+str(e))
                        proddict['NumReviews']= "None" 
                    try:
                        proddict['Price'] = float(prod.find("span",{"class":"entry-price"}).text.replace("€","").replace(",",".").strip())
                        proddict['Crossed_out_Price'] = float(prod.find("span",{"class":"entry-retail-price"}).text.replace("€","").replace(",",".").strip())
                    except Exception as e:
                        try:
                            self.logger.error("Line 102:"+str(e))
                            proddict['Price'] = float(prod.find("span",{"class":"entry-price"}).text.replace("€","").replace(",",".").strip())
                            proddict['Crossed_out_Price'] = "None"
                        except:
                            proddict['Price'] = "None"
                            proddict['Crossed_out_Price'] = "None"
                    try:
                        proddict['Product_name']=prod.find("div",{"class":"result-list-entry-title"}).find("a").text
                        if (db['scrapes'].find({"Source":config['site'],"Product_name":proddict['Product_name']}).count()>0):
                            continue
                    except Exception as e:
                        self.logger.error("Line 112:"+str(e))
                        proddict['Product_name']="None"
                    try:
                        proddict['urltoproduct']=prod.find("div",{"class":"result-list-entry-title"}).find("a")['href']
                    except Exception as e:
                        self.logger.error("Line 101:"+str(e))
                        proddict['urltoproduct']="None"      
                    try:
                        prodsoup=self.get_soup(proddict['urltoproduct'])
                    except Exception as e:
                        self.logger.error("Line 106:"+str(e))
                    try:
                        proddict["Brand"] = prodsoup.find("span",{"id":"product-attribute-brand"}).find("a").text.strip()
                    except Exception as e: 
                        self.logger.error("Line 131:"+str(e))
                        proddict["Brand"] = "None"
                    try:
                        proddict["Format"] = prodsoup.find("span",{"id":"product-attribute-unit"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 131:"+str(e))
                        proddict["Format"] = "None"
                    try:
                        proddict["EAN13"] = prodsoup.find("span",{"id":"product-attribute-pzn"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 131:"+str(e))
                        proddict["EAN13"] = "None"
                    try:
                        proddict["Loyalty"] = prodsoup.find("span",{"class":"text-rp-value"}).text.strip()
                    except Exception as e: 
                        self.logger.error("Line 141:"+str(e))
                        proddict["Loyalty"] = "None" 
        
                    try:
                        proddict["Imagelink"] = prodsoup.find("img",{"data-toggle":"modal"})["src"]
                        proddict['Imagefilename'] = proddict["Imagelink"].split("/")[len(proddict["Imagelink"].split("/"))-1]
                    except Exception as e: 
                        self.logger.error("Line 156:"+str(e))
                        proddict["Imagelink"] = "None" 
                        proddict["Imagefilename"] = "None"
                    try:
                        revs = prodsoup.find_all("div",{"class":"row rating-item"})
                        revlist=[]
                        for rev in revs:
                            revdict=dict()
                            revdict['author']=rev.find("ul").text.split("\n")[2]
                            revdict['date']=rev.find("ul").text.split("\n")[4]
                            revdict['revhead']=rev.find("strong",{"class":"title"}).text
                            revdict['revtext']=rev.find("p").text
                            revlist.append(revdict)
                        proddict["Reviews"]=revlist
                    except Exception as e: 
                        self.logger.error("Line 148:"+str(e))
                        proddict["Reviews"]="None"             
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
        catlist= self.get_catgorylinks(soup)
        if (len(catlist)>0):
            for cat in catlist:
                config['Category']=list(cat.keys())[0]
                url = cat[config['Category']]
                soup = self.get_soup(url)
                seglist = self.get_catgorylinks(soup)
                if (len(seglist)>0):
                    for seg in seglist:
                        config['segment']=list(seg.keys())[0]
                        url = seg[config['segment']]
                        soup = self.get_soup(url)
                        subseglist = self.get_catgorylinks(soup)
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
    
    