###declare libraries
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

site = "https://pharma-express.co/pharmacies/Pharmacie_de_France"
if (platform.system() == "Darwin"):
    driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_mac", chrome_options=chrome_options)
elif (platform.system() == "Linux"):
    driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_linux", chrome_options=chrome_options)
else:
    driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver.exe", chrome_options=chrome_options)
# pull page ...login and get dataframe
driver.get(site)

#### Choosing the product Category link ####
element = driver.find_element_by_css_selector('#pharmacy > div > div.control-bar > a.active').click()
driver.execute_script("arguments[0].click();", element)

element = driver.find_element_by_xpath('//*[@id="pharmacy"]/div/div[1]/a[2]')
driver.execute_script("arguments[0].click();", element)

menulist=[]
#sante
categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
for i in range(0,len(categories)):
    cat = categories[i]
    catdict = {}
    catdict[cat.text] = {}
    currcatkey = cat.text
    driver.execute_script("arguments[0].click();", cat)
    time.sleep(10)
    segments = driver.find_element_by_class_name("nav-area").find_elements_by_tag_name("button")
    for j in range(0,len(segments)):
        seg=segments[j]
        catdict[currcatkey][seg.text]=dict()
        currsegkey = seg.text
        driver.execute_script("arguments[0].click();", seg)
        time.sleep(10)
        brands = driver.find_element_by_class_name("brands-area").find_elements_by_class_name("caps")
        for k in range(0,len(brands)):
            brand=brands[k]
            catdict[currcatkey][currsegkey][brand.text] = dict()
            currbrandkey = brand.text
            driver.execute_script("arguments[0].click();", brand.find_element_by_tag_name("a"))
            time.sleep(15)
            catdict[currcatkey][currsegkey][currbrandkey]['page']=driver.page_source
            menulist.append(catdict)
            driver.get(site)
            element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
            driver.execute_script("arguments[0].click();", element)
            time.sleep(10)
            element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
            driver.execute_script("arguments[0].click();", element)
            categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
            driver.execute_script("arguments[0].click();", categories[i])
            time.sleep(10)
            segments = driver.find_element_by_class_name("nav-area").find_elements_by_tag_name("button")
            driver.execute_script("arguments[0].click();", segments[j])
            time.sleep(10)
        driver.get(site)
        element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
        driver.execute_script("arguments[0].click();", element)
        time.sleep(10)
        element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
        driver.execute_script("arguments[0].click();", element)
        time.sleep(10)
        categories = driver.find_element_by_class_name("dropdown-menu").find_elements_by_tag_name("a")
        driver.execute_script("arguments[0].click();", categories[i])
        time.sleep(10)
    driver.get(site)
    element = driver.find_element_by_xpath('/html/body/nav[1]/div/div[1]/div[1]/a')
    driver.execute_script("arguments[0].click();", element)
    time.sleep(10)
    element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/a')
    driver.execute_script("arguments[0].click();", element)
    time.sleep(10)


        #driver.execute_script("arguments[0].click();", seg)
    menulist.append(catdict)
    driver.get(site)




driver.execute_script("arguments[0].click();", element)

# Sante Products categories
element = driver.find_element_by_xpath('//*[@id="page_custom"]/div[1]/div[1]').find_elements_by_tag_name("button")
driver.execute_script("arguments[0].click();", element[1])


# Sante product catalog
brands = driver.find_element_by_xpath('//*[@id="page_custom"]/div/div[2]').find_elements_by_class_name("caps")
driver.execute_script("arguments[0].click();", brands[1].find_element_by_tag_name("a"))

# sante Product Description
Product = driver.find_element_by_xpath('//*[@id="page_custom"]/div[2]').find_elements_by_class_name("row")
driver.execute_script("arguments[0].click();", brands[1].find_element_by_tag_name("a"))


brands = driver.find_element_by_xpath('//*[@id="page_custom"]/div/div[1]').find_elements_by_class_name("nav-area")
driver.execute_script("arguments[0].click();", brands[1].find_element_by_tag_name("a"))


# Beauty & Hygiene

element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/ul/li[3]/a')
driver.execute_script("arguments[0].click();", element)

# Beauty & Hygiene Product Categories

element = driver.find_element_by_xpath('//*[@id="page_custom"]/div[1]/div[1]').find_elements_by_tag_name("button")
driver.execute_script("arguments[0].click();", element[1])

# Beauty & Hygiene Product catalog

brands = driver.find_element_by_xpath('//*[@id="page_custom"]/div/div[2]').find_elements_by_class_name("caps")
driver.execute_script("arguments[0].click();", brands[1].find_element_by_tag_name("a"))






#Maternity and bebe

element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/ul/li[5]/a')
driver.execute_script("arguments[0].click();", element)

# Nature and Bio

element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/ul/li[9]/a')
driver.execute_script("arguments[0].click();", element)

# Mainenet & domicile



element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/ul/li[11]/a')
driver.execute_script("arguments[0].click();", element)


# Animaux


element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/ul/li[13]/a')
driver.execute_script("arguments[0].click();", element)

#  Medicaments


element = driver.find_element_by_xpath('//*[@id="bs-example-navbar-collapse-1"]/ul/li[6]/ul/li[15]/a')
driver.execute_script("arguments[0].click();", element)

driver.quit()