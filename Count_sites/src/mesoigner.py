from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import platform
from bs4 import BeautifulSoup
import pandas as pd
import tqdm

zips = pd.read_csv("data/france_zips.csv")

print fr_zip

chrome_options = Options()
chrome_options.add_argument('--dns-prefetch-disable')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--lang=en-US')
#chrome_options.add_argument('--headless')
if (platform.system() == "Darwin"):
    driver = webdriver.Chrome("./chromedrivers/chromedriver_mac",
                              chrome_options=chrome_options)
elif (platform.system() == "Linux"):
    driver = webdriver.Chrome("./chromedrivers/chromedriver_linux",chrome_options=chrome_options)
else:
    driver = webdriver.Chrome("./chromedrivers/chromedriver.exe",
                              chrome_options=chrome_options)
driver.get('https://www.mesoigner.fr/trouver-une-pharmacie/code-postal')
retdict=dict()
#j=i
for i in tqdm.tqdm(range(0,len(zips.zip))):
    inputElement = driver.find_element_by_id("find-zipcode")
    inputElement.send_keys(zips.zip[i])
    driver.find_element_by_css_selector('body > div.container.block > div > form > div > div > span > button').click()
    soup = BeautifulSoup(driver.page_source)
    retdict[str(zips.zip[i])]=list(map(lambda x: x['href'],soup.find_all("a",{"class":"btn"})))

driver.quit()

driver.fin

import pickle

a = {'hello': 'world'}

with open('filename.pickle', 'wb') as handle:
    pickle.dump(retdict, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('filename.pickle', 'rb') as handle:
    b = pickle.load(handle)

print a == b