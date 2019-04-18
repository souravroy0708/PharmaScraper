import requests
import urllib
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
sites = []
#loc = {}
# find out zip code of url
locat = urls[i].replace(url,"")

for url in urls:
    if ('www.' in url):
        page = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(page)
        for item in soup.find_all("li",{"style":"margin-bottom: 10px"}):
            for link in item.find_all("a"):
                if ("https:" in link['href']):
                    sites.append(link['href'])
                else:
                    sites.append("https://www.pharmanity.com" + link['href'])
    elif ():
        pass


for site in sites:
    page = urllib.request.urlopen(site).read()
    soup = BeautifulSoup(page)
    soup.find("strong",{"itemprop":"name"})
    break


site =sites[0]
page = urllib.request.urlopen(site).read()
soup = BeautifulSoup(page)
namelist =[]

tags = soup.find_all((("h2"),{"class":"ek-bodiv"})).find("div").find("main").find("ul").find_all("li",recursive=False)
for tag in tags:
    print(tag.text)


tags = soup.find_all("g",{"transform":"scale(0.8605851979345955) translate(13.87999999999998, 0)"})



tags = soup.find_all("div",{"class": "col-sm-7"},"<a")
for i in range(0,len(tags),2):
    namelist.append(tags[i].text)

    print(tag.text)
addlist = []
for i in range(1, len(tags), 2):
    addlist.append(tags[i].text)

tags = soup.find_all("body", {"class": "ek-bodiv"})

from requests_html import HTMLSession
session = HTMLSession()
r = session.get('https://python.org/')

children = tags.findChildren("h2", recursive=False)

for tag in tags:
    print(tag.text)


