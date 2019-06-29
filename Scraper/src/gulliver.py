# -*- coding: utf-8 -*-
###declare libraries
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import json
import os
import logging
import platform
import time
import threading
from bs4 import BeautifulSoup
import pymongo
# define chrome options
chrome_options = Options()
chrome_options.add_argument('--dns-prefetch-disable')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--lang=en-US')
chrome_options.add_argument('--headless')
prefs = {"download.default_directory": os.getcwd()}


# define product page extraction class
class gulliver(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler(
            "logs/" + config['template'] + "_" + config["site"].replace("/", "_").replace(".", "_").replace(":",
                                                                                                            "") + ".log")
        logger_handler.setLevel(logging.DEBUG)

        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)

        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)
        self.logger.info('Completed configuring logger()!')

    def send_data(self,config,pagesoup):
        client = pymongo.MongoClient(self.config["mongolink"])
        db = client[str(self.config['db'])]
        prods= pagesoup.find("div",{"id":"page_custom"}).find_all("div",{"class":"col-sm-6"})
        for prod in prods:
            proddict=config.copy()
            try:
                proddict['Product_name'] = prod.find("h4",{"class":"name-area"}).text
                if (db['scrapes'].find(
                        {"Source": self.config['site'], "Product_name": proddict['Product_name']}).count() > 0):
                    continue
            except:
                self.logger.error("Line 100:" + str(e))
                proddict['Product_name'] = "None"
            try:
                proddict['Price'] = float(
                    prod.find("span", {"class": "price product-price"}).text.replace("\\€", ".").strip())
                proddict['Crossed_out_Price'] = "None"
            except Exception as e:
                self.logger.error("Line 122:" + str(e))
                proddict['Price'] = float(prod.find("p", {"class": "price-area"}).text.replace("\\€","").replace(",","").strip())
                proddict['Crossed_out_Price'] = "None"
            try:
                proddict['Imagelink'] = prod.find("img")['src']
                proddict['Imagefilename'] = proddict['Imagelink'].split("/")[len(proddict['Imagelink'].split("/")) - 1]
            except:
                self.logger.error("Line 122:" + str(e))
                proddict['Imagelink'] = "None"
                proddict['Imagefilename'] = "None"
            db[str(self.config['collection'])].insert_one(proddict)
        client.close()
        pass



    def run(self):
        config = self.config
        site = config['urls']
        if (platform.system() == "Darwin"):
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_mac", chrome_options=chrome_options)
        elif (platform.system() == "Linux"):
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_linux", chrome_options=chrome_options)
        else:
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver.exe", chrome_options=chrome_options)
        # pull page ...login and get dataframe
        driver.get(site)
        #### Choosing the product Category link ####
        element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
        driver.execute_script("arguments[0].click();", element)
        element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
        driver.execute_script("arguments[0].click();", element)
        categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
        for i in range(0, len(categories)):
            cat = categories[i]
            currcatkey = cat.text
            driver.execute_script("arguments[0].click();", cat)
            time.sleep(10)
            segments = driver.find_element_by_class_name("nav-area").find_elements_by_tag_name("button")
            for j in range(0, len(segments)):
                seg = segments[j]
                currsegkey = seg.text
                driver.execute_script("arguments[0].click();", seg)
                time.sleep(10)
                brands = driver.find_element_by_class_name("brands-area").find_elements_by_class_name("caps")
                for k in range(0, len(brands)):
                    brand = brands[k]
                    currbrandkey = brand.text
                    driver.execute_script("arguments[0].click();", brand.find_element_by_tag_name("a"))
                    time.sleep(15)
                    try:
                        page = driver.page_source
                        configdict ={}
                        configdict['Source']=site
                        configdict['Brand'] = currbrandkey
                        configdict['segment'] = currsegkey
                        configdict['Category'] = currcatkey
                        configdict['template'] = "gulliver"
                        self.send_data(configdict, BeautifulSoup(page))
                    except:
                        driver.quit()
                        driver.get(site)
                        element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(10)
                        element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
                        driver.execute_script("arguments[0].click();", element)
                        categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
                        driver.execute_script("arguments[0].click();", categories[i])
                        time.sleep(10)
                        segments = driver.find_element_by_class_name("nav-area").find_elements_by_tag_name("button")
                        driver.execute_script("arguments[0].click();", segments[j])
                        time.sleep(10)
                        brands = driver.find_element_by_class_name("brands-area").find_elements_by_class_name("caps")
                        continue
                    driver.get(site)
                    element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
                    driver.execute_script("arguments[0].click();", element)
                    time.sleep(10)
                    element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
                    driver.execute_script("arguments[0].click();", element)
                    categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
                    driver.execute_script("arguments[0].click();", categories[i])
                    time.sleep(10)
                    segments = driver.find_element_by_class_name("nav-area").find_elements_by_tag_name("button")
                    driver.execute_script("arguments[0].click();", segments[j])
                    time.sleep(10)
                    brands = driver.find_element_by_class_name("brands-area").find_elements_by_class_name("caps")
                driver.get(site)
                element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
                driver.execute_script("arguments[0].click();", element)
                time.sleep(10)
                element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
                driver.execute_script("arguments[0].click();", element)
                time.sleep(10)
                categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
                driver.execute_script("arguments[0].click();", categories[i])
                time.sleep(10)
                segments = driver.find_element_by_class_name("nav-area").find_elements_by_tag_name("button")
            driver.get(site)
            element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
            driver.execute_script("arguments[0].click();", element)
            time.sleep(10)
            element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
            driver.execute_script("arguments[0].click();", element)
            time.sleep(10)
            categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
        pass