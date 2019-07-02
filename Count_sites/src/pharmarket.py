#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhisekray
"""

from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import pandas as pd
import logging
import threading

class phrama_marketplay(threading.Thread):
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

    def data_pharmarket(self):
        try:
            urls=self.config['sites']
            website3=[]
            phone3=[]
            address3=[]
            name3=[]

            for input in tqdm(range(0,len(urls))):
                web = urls[input]
                url_request2=requests.get(urls[input])
                soup2=BeautifulSoup(url_request2.content, 'html.parser')
                nam=soup2.find('span',attrs={'class':'main'}).text
                add=soup2.find('span',attrs={'class':'zip'}).text+' '+soup2.find('span',attrs={'class':'city'}).text
                ph=soup2.find('div',attrs={'class':'tel'}).text
                website3.append(web)
                phone3.append(ph)
                address3.append(add)
                name3.append(nam)

            df3=pd.DataFrame()
            df3['Name']=name3
            df3['Address']=address3
            df3['Telephone']=phone3
            df3['Website']=website3
            df3['Pharma']='pharmarket'
            return df3
        except Exception as e:
            self.logger.info("Failed url:" + self.config['sites'])

    def run(self):

        data_pharma3 = self.data_pharmarket().to_dict(orient='records')
        myclient = MongoClient("mongodb://localhost:27017/")
        db=myclient['allpharmacy']
        col3=db['pharmacy']
        try:
            col3.insert_many(data_pharma3)
        except Exception as e:
            print(str(e))
