from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import json
import os
import platform
import time
# define chrome options
chrome_options = Options()
chrome_options.add_argument('--dns-prefetch-disable')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--lang=en-US')
prefs = {"download.default_directory": os.getcwd()}


# Parameters ...these parameters should go to the config file
params = dict()
params["FirstName"] = "Abhishek"
params["LastName"] = "Rajan"
params["Phone"] = "8884784503"
params["DOB"] = "28/10/1982"
params["Email"] = "callabhisheksinha@gmail.com"
params["Password"] = "Achilles@2019"


sites_Pharmavie = 'https://grandepharmaciedesmarechaux.pharmavie.fr/inscription/carte'




for site in sites_Pharmavie:
    # chrome_options.add_argument('--headless')
    try:
        if (platform.system() == "Darwin"):
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_mac", chrome_options=chrome_options)
        elif (platform.system() == "Linux"):
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_linux", chrome_options=chrome_options)
        else:
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver.exe", chrome_options=chrome_options)
        # pull page ...login and get dataframe
        driver.get(site)
        time.sleep(5)

        email_input = driver.find_element_by_id("customerregister_email")
        email_input.send_keys(params["Email"])

        FirstName_input =  driver.find_element_by_id("customerregister_firstname")
        FirstName_input.send_keys(params["FirstName"])

        LastName_input = driver.find_element_by_id("customerregister_lastname")
        LastName_input.send_keys(params["LastName"])

        Phone_input = driver.find_element_by_id("customerregister_phone")
        Phone_input.send_keys(params["Phone"])

        DOB_input = driver.find_element_by_id("customerregister_birthday")
        DOB_input.send_keys(params["DOB"])

        Password_input = driver.find_element_by_id("customerregister_plainPassword")
        Password_input.send_keys(params["Password"])

        driver.find_element_by_id('customerregister_terms').click()
        time.sleep(2)
        element = driver.find_element_by_css_selector("#customerregister_newsletter > div:nth-child(2) > label")
        driver.execute_script("arguments[0].click();", element)
        element = driver.find_element_by_css_selector('body > main > div.py-4.py-lg-5.main-content > div:nth-child(2) > form > button')
        driver.execute_script("arguments[0].click();", element)
        driver.quit()
    except:
        print(site)
        driver.quit()
