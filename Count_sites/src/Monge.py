from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from pymongo import MongoClient


url1='https://pharmacie-monge.fr/trouvez-votre-pharmacie'
url_request1=requests.get(url1)
soup1=BeautifulSoup(url_request1.content, 'html.parser')

data1=soup1.find_all('div',class_=re.compile("bloc_pharmacie"))

address4=[]
telephone4=[]
name4=[]
website4=[]

for input in tqdm(range(0,len(data1))):
    ad=data1[input].find('div',class_=re.compile('col-xs-12 col-md-3 info')).text.split('\n\t')[2].split('\n  ')[1]+data1[input].find('div',class_=re.compile('col-xs-12 col-md-3 info')).text.split('\n\t')[2].split('\n  ')[2] #adress
    tel=data1[input].find('div',class_=re.compile('col-xs-12 col-md-3 info')).text.split('\n\t')[2].split('\n  ')[3]
    nam=data1[input].find('div',class_=re.compile('col-xs-12 col-md-3 info')).text.split('\n\t')[2].split('\n  ')[4]
    address4.append(ad)
    telephone4.append(tel)
    name4.append(nam)
    try:
        web = data1[input].find('a', attrs={'class': re.compile('btn btn_achat')})['href']
        website4.append(web)
    except:
        website4.append(' ')


df4=pd.DataFrame()
df4['Name']=name4
df4['Address']=address4
df4['Telephone']=telephone4
df4['Website']=website4
df4['Pharma']='aprium_monge'


data_pharma4 = df4.to_dict(orient='records')

myclient = MongoClient("mongodb://localhost:27017/")
db=myclient['allpharmacy']
col4=db['pharmacy']
try:
    col4.insert_many(data_pharma4)
except Exception as e:
    print(str(e))