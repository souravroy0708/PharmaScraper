#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhisekray
"""
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import re
import logging
import threading

class phrama_lafaytte(threading.Thread):
      def __init__(self,config):
            threading.Thread.__init__(self)
            self.config = config
            # Create the Logger
            self.logger = logging.getLogger(__name__)

            self.logger.setLevel(logging.INFO)

            # Create the Handler for logging data to a file
            logger_handler = logging.FileHandler("logs/"+config['template']+".log")

            logger_handler.setLevel(logging.DEBUG)

             # Create a Formatter for formatting the log messages
            logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

            # Add the Formatter to the Handler
            logger_handler.setFormatter(logger_formatter)

            # Add the Handler to the Logger
            self.logger.addHandler(logger_handler)
            self.logger.info('Completed configuring logger()!')

      def data_lafaytte(self,list):
            Name1=[]
            adress1=[]
            Telephone1=[]
            website1=[]
            for link in tqdm(range(0,len(list))):
                  url=self.config["site"]+str(list[link])
                  url_request=requests.get(url)
                  soup=BeautifulSoup(url_request.content, 'html.parser')
                  hello=soup.findAll('div',attrs={"class":"texte_adresse"})
                  hello_1=[x.find('p') for x in hello]
                  for i in range(0,len(hello_1)):
                        text=hello_1[i].text.split('\n')[0]
                        add=' '.join([hello_1[i].text.split('\n')[1],hello_1[i].text.split('\n')[2]])
                        tel=hello_1[i].text.split('\n')[3]
                        Name1.append(text)
                        adress1.append(add)
                        Telephone1.append(tel)
                        try:
                              website1.append(hello_1[i].find('a')['href'])
                        except Exception:
                              website1.append("")

            adress1=[re.sub(r"[\t?\' ']+",' ' ,adress1[i]) for i in range(0,len(adress1))]
            Telephone1=[re.sub(r"Tél :",' ' ,Telephone1[i]) for i in range(0,len(Telephone1))]
            df1=pd.DataFrame()
            df1['Name']=Name1
            df1['Address']=adress1
            df1['Telephone']=Telephone1
            df1['Website']=website1
            df1['Pharma']='lafaytte'
            return df1

      def run(self):
            list = self.config["urlsuffix"]
            data_pharma1 = self.data_lafaytte(list).to_dict(orient='records')
            myclient = MongoClient("mongodb://localhost:27017/")
            db=myclient['allpharmacy']
            col1=db['pharmacy']
            try:
                  col1.insert_many(data_pharma1)
            except Exception as e:
                  print(str(e))


