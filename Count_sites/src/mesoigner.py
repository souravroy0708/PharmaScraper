from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import platform
from bs4 import BeautifulSoup
import pandas as pd
import tqdm
from pymongo import MongoClient

pin_data = pd.read_csv('/home/imransk/Downloads/FR.txt',header=None)

zips=[]

for i in range(0,len(pin_data[0])):
    pin=pin_data[0][i].split('\t')[1]
    zips.append(pin)


print(zips)

chrome_options = Options()
chrome_options.add_argument('--dns-prefetch-disable')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--lang=en-US')
#chrome_options.add_argument('--headless')
if (platform.system() == "Darwin"):
    driver = webdriver.Chrome("./chromedrivers/chromedriver_mac",
                              chrome_options=chrome_options)
elif (platform.system() == "Linux"):
    driver = webdriver.Chrome("/home/imransk/Documents/Repos/PharmaScraper/Count_sites/chromedrivers/chromedriver_linux",chrome_options=chrome_options)
else:
    driver = webdriver.Chrome("./chromedrivers/chromedriver.exe",
                              chrome_options=chrome_options)
driver.get('https://www.mesoigner.fr/trouver-une-pharmacie/code-postal')

#j=i
for i in tqdm.tqdm(range(0,len(zips))):
    inputElement = driver.find_element_by_id("find-zipcode")
    inputElement.send_keys(zips[i])
    driver.find_element_by_css_selector('body > div.container.block > div > form > div > div > span > button').click()
    soup = BeautifulSoup(driver.page_source)
    retdict = pd.DataFrame()
    retdict['Website']=list(map(lambda x: x['href'],soup.find_all("a",{"class":"btn"})))
    retdict['zips'] = str(zips[i])
    retdict['Pharma'] = "mesoigner"
    retdict = retdict.to_dict(orient='records')
    myclient = MongoClient("mongodb://localhost:27017/")
    # myclient = MongoClient("mongodb://abhir:AbhiPrecision19@mongodb-712-0.cloudclusters.net:27017/admin")
    db = myclient['allpharmacy']
    col7 = db['pharmacy']
    try:
        col7.insert_many(retdict)
    except Exception as e:
        print(str(e))

    if (i==0):
        finaldf=retdict
    else:
        finaldf=  finaldf.append(retdict,ignore_index=True)
finaldf['Pharma'] ="mesoigner"

data_pharma6= retdict.to_dict(orient='records')

myclient = MongoClient("mongodb://localhost:27017/")
db=myclient['allpharmacy']
col7=db['pharmacy']
try:
    col7.insert_many(data_pharma6)
except Exception as e:
    print(str(e))


#driver.quit()

#driver.fin

#import pickle

#a = {'hello': 'world'}

#with open('filename.pickle', 'wb') as handle:
    #pickle.dump(retdict, handle, protocol=pickle.HIGHEST_PROTOCOL)

#with open('filename.pickle', 'rb') as handle:
#    b = pickle.load(handle)

#print(a == b)