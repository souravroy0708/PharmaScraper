from selenium import webdriver
from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient



data1=pd.read_csv('/home/imransk/Downloads/FR.txt',header=None)

pin_data=[]

for i in range(0,len(data1[0])):
    pin=data1[0][i].split('\t')[1]
    pin_data.append(pin)


Address2 = []
Fax2 = []
Telephone2 = []
Website2 = []
Name2=[]

browser = webdriver.Chrome('/home/imransk/Documents/Repos/PharmaScraper/Count_sites/chromedrivers/chromedriver_linux')

for input in tqdm(range(0,len(pin_data))):
    website_URL = "https://aprium-pharmacie.fr/store"
    browser.get(website_URL)
    element = browser.find_element_by_xpath("//*[@id=\"autocompleteGoogleNavBar\"]")
    element.send_keys(pin_data[input])
    element.submit()

    soup = BeautifulSoup(browser.page_source, 'html')

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

data_pharma2= df2.to_dict(orient='records')

myclient = MongoClient("mongodb://localhost:27017/")
db=myclient['allpharmacy']
col2=db['pharmacy']
try:
    col2.insert_many(data_pharma2)
except Exception as e:
    print(str(e))