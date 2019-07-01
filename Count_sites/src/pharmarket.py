from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import pandas as pd

urls=['https://paris-beaugrenelle.pharmarket.com/',\
        'https://pharmacie-centrale-plan-de-cuques.pharmarket.com/',\
        'https://grande-pharmacie-croix-bleue-nimes.pharmarket.com/',\
        'https://pharmacie-remicourt-saint-quentin.pharmarket.com/',\
        'https://pharmacie-des-ecoles-marignane.pharmarket.com/',\
        'https://pharmacie-des-salines-montmorot.pharmarket.com/',\
        'https://pharmacie-du-rieu-fargniers.pharmarket.com/',\
        'https://pharmacie-mikou-bray-sur-seine.pharmarket.com/',\
        'https://pharmacie-des-salines-ajaccio.pharmarket.com/',\
        'https://pharmacie-morelle-avesnes-aubert.pharmarket.com/',\
        'https://pharmacie-rochefort-saint-michel.pharmarket.com/',\
        'https://pharmacie-souffel-weyersheim.pharmarket.com/',\
        'https://pharmacie-hamani-saint-michel-sur-orge.pharmarket.com/']


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

data_pharma3 = df3.to_dict(orient='records')

myclient = MongoClient("mongodb://localhost:27017/")
db=myclient['allpharmacy']
col3=db['pharmacy']
try:
    col3.insert_many(data_pharma3)
except Exception as e:
    print(str(e))