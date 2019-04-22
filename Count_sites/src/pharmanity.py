import requests
import urllib2
import pymongo
import logging
import threading
import re
from bs4 import BeautifulSoup


url = "https://www.pharmanity.com/pharmacies"
try:
    page = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(page)
except:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)     Chrome/37.0.2049.0 Safari/537.36'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text)

urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', soup.text)[0].split(";")
urls =[i.split("=")[1].replace("'","") if "Region" in i else i.replace("'","") for i in urls]
locsites = {}
#loc = {}
# find out zip code of url
for i in range(0, len(urls)):
    if ('www' in str(urls[i])):
        sites = []
        url = str(urls[i])
        locat = url.split("/")[len(url.split("/"))-1]
        print(locat)
        page = urllib2.urlopen(url)
        soup = BeautifulSoup(page)
        for item in soup.find_all("li", {"style": "margin-bottom: 10px"}):
            for link in item.find_all("a"):
                if ("https:" in link['href']):
                    sites.append(link['href'])
                else:
                    sites.append("https://www.pharmanity.com" + link['href'])
        locsites[locat] = {}
        locsites[locat]["sites"] = sites



for keyl in list(locsites.keys()):
    numl = []
    for site in locsites[keyl]["sites"]:
        url = sites[len(sites)-1]
        page_med = urllib2.urlopen(url)
        soup_med = BeautifulSoup(page_med)
        item_med = soup_med.find_all("a", class_='menuMedi')[0]
        link_part_med = item_med['href']
        link_med = 'https://www.pharmanity.com'+link_part_med
        page_med_2 = urllib2.urlopen(link_med)
        soup_med_2 = BeautifulSoup(page_med_2)
        item_med_2 = soup_med_2.find_all("a", class_='current')[0]
        txt = str(item_med_2.span.text)
        meds =int(txt[txt.find("(")+1:txt.find(")")])
        item_para = soup_med.find_all("a", class_='menuPara')[0]
        link_part_para = item_para['href']
        link_para = 'https://www.pharmanity.com'+link_part_para
        page_para_2 = urllib2.urlopen(link_para)
        soup_para_2 = BeautifulSoup(page_para_2)
        item_para_2 = soup_para_2.find("div", class_='sidebox menuparapharmacie')
        strong_para_2 = item_para_2.find_all("strong")
        pharms = 0
        for strong in strong_para_2:
            no = str(strong.span.text)
            number =int(no[no.find("(")+1:no.find(")")])
            pharms = pharms + number

        final_sum = meds + pharms
        numl.append(final_sum)
    locsites[locat]["numl"] = numl
    print(final_sum)
    print('\n')







