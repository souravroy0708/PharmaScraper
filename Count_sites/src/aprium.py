#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 15:36:57 2018

@author: abhisekray
"""

from selenium import webdriver
from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import logging
import threading

class phrama_aprium(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)

        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler("logs/" + config['template'] + ".log")

        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')
    def data_aprium(self,data):
        pin_data=[]
        for i in range(0,len(data[0])):
            pin=data[0][i].split('\t')[1]
            pin_data.append(pin)

        Address2 = []
        Fax2 = []
        Telephone2 = []
        Website2 = []
        Name2=[]
        browser = webdriver.Chrome('/home/imransk/Documents/Repos/PharmaScraper/Count_sites/chromedrivers/chromedriver_linux')

        for input in tqdm(range(0,len(pin_data))):
            website_URL = self.config['site']
            browser.get(website_URL)
            element = browser.find_element_by_xpath("//*[@id=\"autocompleteGoogleNavBar\"]")
            element.send_keys(pin_data[input])
            element.submit()
            soup = BeautifulSoup(browser.page_source, 'html.parser')
            data=soup.findAll('div',attrs={"class":"container bloc_pharmacie"})
            for i in tqdm(range(0,len(data))):
                fa=data[i].find('div',attrs={"class":"col-xs-12 col-md-3 info"}).text.split('\n')[7]
                add_part1=data[i].find('div',attrs={"class":"col-xs-12 col-md-3 info"}).text.split('\n')[3]
                add_part2=data[i].find('div',attrs={"class":"col-xs-12 col-md-3 info"}).text.split('\n')[5]
                add=add_part1+add_part2
                nam=data[i].find('span',attrs={"class":"tittle_logo"}).text
                tel=data[i].find('div',attrs={"class":"col-xs-12 col-md-3 info"}).text.split('\n')[6]
                Fax2.append(fa)
                Address2.append(add)
                Telephone2.append(tel)
                Name2.append(nam)
                try:
                    web = data[i].findAll('a')
                    for item in web:
                        if ("aprium-pharmacie.fr" in item['href']):
                            Website2.append(item['href'])
                            break
                except Exception:

                    Website2.append(' ')

        df2=pd.DataFrame()
        df2['Name']=Name2
        df2['Address']=Address2
        df2['Telephone']=Telephone2
        df2['Website']=Website2
        df2['Pharma']='aprium'

        df2.sort_values("Name", inplace = True)
        df2.drop_duplicates(inplace = True)

        return df2

    def run(self):

        data1 = pd.read_csv('/home/imransk/Downloads/FR.txt', header=None)
        data_pharma2= self.data_aprium(data1).to_dict(orient='records')
        myclient = MongoClient("mongodb://localhost:27017/")
        db=myclient['allpharmacy']
        col2=db['pharmacy']
        try:
            col2.insert_many(data_pharma2)
        except Exception as e:
            print(str(e))

