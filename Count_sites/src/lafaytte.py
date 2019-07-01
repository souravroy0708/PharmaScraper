from bs4 import BeautifulSoup
import requests
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import re

list=['Alsace','Aquitaine','Auvergne','Bretagne','Centre','Champagne-ardenne','Haute-normandie','Ile-de-france',\
      'Languedoc-roussillon','Limousin','Lorraine','Midi-pyrénées','Nord-pas-de-calais','Pays-de-la-loire',\
      'Picardie','Poitou-charentes','Provence-alpes-cote-d\'azur','Rhône-alpes']

Name1=[]
adress1=[]
Telephone1=[]
website1=[]

for link in tqdm(range(0,len(list))):
      url='https://www.pharmacielafayette.com/front_internaute.php?zone='+str(list[link])
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

data_pharma1 = df1.to_dict(orient='records')

myclient = MongoClient("mongodb://localhost:27017/")
db=myclient['allpharmacy']
col1=db['pharmacy']
try:
      col1.insert_many(data_pharma1)
except Exception as e:
      print(str(e))