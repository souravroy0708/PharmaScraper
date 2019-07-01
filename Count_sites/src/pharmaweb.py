from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
import pandas as pd
import itertools
import re
from pymongo import MongoClient

web=pd.read_csv('/home/imransk/Documents/Repos/PharmaScraper/Miscellaneous/web.csv',header=None)
browser1 = webdriver.Chrome('/home/imransk/Documents/Repos/PharmaScraper/Count_sites/chromedrivers/chromedriver_linux')

for inpu in tqdm(range(0,len(web[1]))):

    website_URL1 ="http://www.whois-raynette.fr/"
    browser1.get(website_URL1)
    element=browser1.find_element_by_xpath("/html/body/div/div/div/div[1]/div/form/p/input[1]")
    element.send_keys(web[1][inpu])
    element.submit()
    soup3 = BeautifulSoup(browser1.page_source, 'html')

    try:
        ip= soup3.find('tr').text.split(':')[1].split('\n')[1]
    except:
        continue

    url = 'https://www.virustotal.com/ui/ip_addresses/'+str(ip)+'/resolutions?limit=40'
    try:
        resp = requests.get(url=url)
    except:
        print('recapcha_error')
        continue

    dat = []

    try:
        data = resp.json()
        dat.append(data['data'])
    except:
        pass
    try:
        while True:
            url = data['links']['next']
            resp = requests.get(url=url)
            data = resp.json()
            dat.append(data['data'])
    except:
        pass

    merged=list(itertools.chain(*dat))
    merged= list(map(lambda x: x['attributes'], merged))
    tempdf = pd.DataFrame.from_dict(merged)
    tempdf['Name'] = web[0][inpu]
    tempdf['Website'] = web[1][inpu]
    if (inpu==0):
        finaldf = tempdf
    else:
        finaldf= finaldf.append(tempdf,ignore_index=True)
    print(len(finaldf))



finaldf.drop(columns=['date','ip_address','resolver'],inplace=True)

data_pharma5= finaldf.to_dict(orient='records')

myclient = MongoClient("mongodb://localhost:27017/")
db=myclient['allpharmacy']
col5=db['pharmacy']
try:
    col5.insert_many(data_pharma5)
except Exception as e:
    print(str(e))






