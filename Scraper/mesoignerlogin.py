# -*- coding: utf-8 -*-

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







# Parameters ...these parameters should go to the config file
params = dict()
params["FirstName"] = "Abhishek"
params["LastName"] = "Rajan"
params["Phone"] = "8884784503"
params["DOB"] = "28/10/1982"
params["Email"] = "callabhisheksinha@gmail.com"
params["Password"] = "Achilles@2019"


sites_mesoigner = ["https://grandepharmaciedesmarechaux.pharmavie.fr/inscription/carte","https://pharmacieducentre-illzach.pharmavie.fr/inscription/carte","https://pharmaciedubernard.pharmavie.fr/inscription/carte","https://grandepharmaciedepoissy.pharmavie.fr/inscription/carte","https://grandepharmaciedesmarechaux.pharmavie.fr/inscription/carte","https://pharmaciebonnemie.pharmavie.fr/inscription/carte","https://pharmacieboth.pharmavie.fr/inscription/carte","https://pharmaciedargent.mesoigner.fr/inscription/carte","https://pharmaciedelafontaine-sermaize.pharmavie.fr/inscription/carte","https://pharmaciedelaloire.pharmavie.fr/inscription/carte","https://pharmaciedeletoile-jouelestours.pharmavie.fr/inscription/carte","https://pharmaciedubernard.pharmavie.fr/inscription/carte","https://pharmaciedumarche-saintpriest.pharmavie.fr/inscription/carte","https://pharmacieduparcmoirans.pharmavie.fr/inscription/carte","https://pharmaciedupont-versailles.pharmavie.fr/inscription/carte","https://pharmacieguillemet-savenay.pharmavie.fr/inscription/carte","https://pharmacieinternationale-nice.pharmavie.fr/inscription/carte","https://pharmacielagoutte.pharmavie.fr/inscription/carte","https://pharmacielarkin.pharmavie.fr/inscription/carte","https://pharmaciemercier.pharmavie.fr/inscription/carte","https://pharmaciepatton.pharmavie.fr/inscription/carte","https://pharmaciepothier.pharmavie.fr/inscription/carte","https://pharmacierigaud-82800.pharmavie.fr/inscription/carte","https://pharmaciesaintemarie-saintavold.pharmavie.fr/inscription/carte","https://pharmaciedubourgdenis.pharmavie.fr/inscription/carte"]




for site in sites_mesoigner:
    # chrome_options.add_argument('--headless')
    try:
        if (platform.system() == "Darwin"):
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_mac", chrome_options=chrome_options)
        elif (platform.system() == "Linux"):
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_linux", chrome_options=chrome_options)
        else:
            driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver.exe", chrome_options=chrome_options)
        # pull page ...login and get dataframe
        driver.get(site)
        time.sleep(5)

        email_input = driver.find_element_by_id("customerregister_email")
        email_input.send_keys(params["Email"])

        FirstName_input =  driver.find_element_by_id("customerregister_firstname")
        FirstName_input.send_keys(params["FirstName"])

        LastName_input = driver.find_element_by_id("customerregister_lastname")
        LastName_input.send_keys(params["LastName"])

        Phone_input = driver.find_element_by_id("customerregister_phone")
        Phone_input.send_keys(params["Phone"])

        DOB_input = driver.find_element_by_id("customerregister_birthday")
        DOB_input.send_keys(params["DOB"])

        Password_input = driver.find_element_by_id("customerregister_plainPassword")
        Password_input.send_keys(params["Password"])

        driver.find_element_by_id('customerregister_terms').click()
        time.sleep(2)
        element = driver.find_element_by_css_selector("#customerregister_newsletter > div:nth-child(2) > label")
        driver.execute_script("arguments[0].click();", element)
        element = driver.find_element_by_css_selector('body > main > div.py-4.py-lg-5.main-content > div:nth-child(2) > form > button')
        driver.execute_script("arguments[0].click();", element)
        driver.quit()
    except:
        print(site)
        driver.quit()


        #### login

sites_mesoigner_login = ["https://pharmacie-durmenach.mesoigner.fr/connexion","https://pharmaciedebias.mesoigner.fr/connexion","https://pharmaciehergueux-baumelesdames.mesoigner.fr/connexion","https://pharmaciemirande.mesoigner.fr/connexion","https://accoceberry.mesoigner.fr/connexion","https://pharmacie-arago-pessac.mesoigner.fr/connexion","https://pharmacie-belhomme-saint-medard.mesoigner.fr/connexion","https://pharmaciebreton-daumesnil-paris.mesoigner.fr/connexion","https://pharmacie-cavignac-gafsi.mesoigner.fr/connexion","https://pharmacie-centrale-bruguieres-herboristerie.mesoigner.fr/connexion","https://pharmaciedelacroixbleue.mesoigner.fr/connexion","https://pharmaciedelamer.mesoigner.fr/connexion","https://pharmaciedelanationale-saint-jory.mesoigner.fr/connexion","https://pharmaciedelaplace-paris.mesoigner.fr/connexion","https://pharmaciedelasave-montaigut-sur-save.mesoigner.fr/connexion","https://pharmaciedelavache-le-bouscat.mesoigner.fr/connexion","https://pharmaciedelavenuedemuret-toulouse.mesoigner.fr/connexion","https://pharmaciedubourg.mesoigner.fr/connexion","https://pharmacieducentreclamart.mesoigner.fr/connexion","https://pharmacieducentre-marmande.mesoigner.fr/connexion","https://pharmacieduroule.mesoigner.fr/connexion","https://pharmacie-hourtin.mesoigner.fr/connexion","https://pharmaciejardindesplantes-toulouse.mesoigner.fr/connexion","https://pharmacie-longchamps.mesoigner.fr/connexion","https://pharmacie-meignie.mesoigner.fr/connexion","https://pharmacie-pessacfrance.mesoigner.fr/connexion","https://pharmacierenaudie-albi.mesoigner.fr/connexion","https://pharmagarerueil92.mesoigner.fr/connexion","https://pharmacie-toulenne.rocade.fr/connexion","https://pharmacie-audenge.rocade.fr/connexion","https://pharmacie-biganos.rocade.fr/connexion","https://pharmacie-ambares.rocade.fr/connexion","https://pharmaciedelacourondelle.rocade.fr/connexion"]
for site in sites_mesoigner_login:
    if (platform.system() == "Darwin"):
        driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_mac", chrome_options=chrome_options)
    elif (platform.system() == "Linux"):
        driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver_linux", chrome_options=chrome_options)
    else:
        driver = webdriver.Chrome(os.getcwd() + "/chromedrivers/chromedriver.exe", chrome_options=chrome_options)
    # pull page ...login and get dataframe
    driver.get(site)
    time.sleep(5)


            email_input = driver.find_element_by_id("username")
            email_input.send_keys(params["Email"])

            Password_input = driver.find_element_by_id("password")
            Password_input.send_keys(params["Password"])
            element = driver.find_element_by_css_selector('body > main > div.py-4.py-lg-5.main-content > div:nth-child(2) > form > button')
            driver.execute_script("arguments[0].click();", element)




#### usingig urlib##
import http.cookiejar
import urllib.parse
import urllib.request

jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

payload = urllib.parse.urlencode({"_username": "callabhisheksinha@gmail.com",
                                  "_password": "Achilles@2019",
                                  "_remember_me": "1",
                                  "_remember_me": "",
                                  "login": "Login"}).encode("utf-8")
response = opener.open("https://pharmacie-durmenach.mesoigner.fr/connexion", payload)
data = response.read()

# print the HTML after the request
print(bytes(str(data), "utf-8").decode("unicode_escape"))

import requests

# Fill in your details here to be posted to the login form.
payload = {"_username": "callabhisheksinha@gmail.com","_password": "Achilles@2019"}

# Use 'with' to ensure the session context is closed after use.
with requests.Session() as s:
    p = s.post('https://pharmaciedebias.mesoigner.fr/connexion', data=payload)
    # print the html returned or something more intelligent to see if it's a successful login page.
    print(p.text)

    # An authorised request.
    r = s.get('https://pharmaciedebias.mesoigner.fr/medicaments-parapharmacie/284-transit-tube-digestif')
    print(r.text)
    with open('somefile.html', 'w') as the_file:
        the_file.write(r.text)



driver.find_element_by_id('identifierNext').click()
time.sleep(30)
pwd_input = driver.find_element_by_xpath('//*[@id="password"]/div[1]/div/div[1]/input')
pwd_input.send_keys(params["gapwd"])
driver.find_element_by_id('passwordNext').click()
time.sleep(30)
driver.switch_to.frame(driver.find_element_by_id("galaxyIframe"))
nids = 1 + int(driver.find_element_by_class_name("C_PAGINATION_ROWS_LONG").text.split(" of ")[
                   len(driver.find_element_by_class_name("C_PAGINATION_ROWS_LONG").text.split(" of ")) - 1]) / 5000
for i in range(0, nids):
    if (i == 0):
        nrows = driver.find_element_by_class_name("_GAl3b").find_element_by_tag_name("select")
        nrows.send_keys("5000")
        time.sleep(30)
        tbl = driver.find_element_by_id("ID-rowTable").get_attribute('outerHTML')
        df = pd.read_html(tbl, converters={"Sessions": str})[0]
        cols = list(df.columns)
        df.drop(['Client Id'], axis=1, inplace=True)
        df.columns = cols[0:(len(cols) - 1)]
    else:
        driver.find_element_by_class_name("_GAPW").click()
        time.sleep(30)
        tbl = driver.find_element_by_id("ID-rowTable").get_attribute('outerHTML')
        tempdf = pd.read_html(tbl, converters={"Sessions": str})[0]
        cols = list(tempdf.columns)
        tempdf.drop(['Client Id'], axis=1, inplace=True)
        tempdf.columns = cols[0:(len(cols) - 1)]
        df = df.append(tempdf)

# Loop through the dataframe and get the json files
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["GADB"]
col = db["rawgasessions"]
for i in range(i, len(df)):
    if not os.path.exists("data/" + str(df['Client Id'].iloc[i])):
        os.makedirs("data/" + str(df['Client Id'].iloc[i]))
    tempstart = startdate
    tempend = tempstart + timedelta(days=7)
    try:
        while (tempend < enddate):
            url = "https://analytics.google.com/analytics/web/?authuser=0#/report/visitors-user-activity/a177871w3490836p54583939/_u.date00=" + str(
                tempstart).replace("-", "") + "&_u.date01=" + str(tempend).replace("-", "") + "&_r.userId=" + str(
                df['Client Id'].iloc[i]) + "&_r.userListReportStates=%3F_u.date00=" + str(tempstart).replace("-",
                                                                                                             "") + "%2526_u.date01=" + str(
                tempend).replace("-",
                                 "") + "%2526explorer-table.plotKeys=%5B%5D%2526explorer-table.rowStart=0%2526explorer-table.rowCount=5000&_r.userListReportId=visitors-legacy-user-id/"
            driver.get(url)
            time.sleep(30)
            driver.switch_to.frame(driver.find_element_by_id("galaxyIframe"))
            nrows = driver.find_element_by_class_name("_GAfR").find_element_by_tag_name("select")
            nrows.send_keys("500")
            time.sleep(30)
            driver.find_element_by_class_name('_GARPb').find_elements_by_tag_name('button')[6].click()
            time.sleep(3)
            filename = max([f for f in glob.glob('./*.json')], key=os.path.getctime).replace(".\\", "")
            os.rename(filename, str(tempstart) + "_" + str(tempend) + ".json")
            tempend = tempstart + timedelta(days=7)
            tempstart = tempend + timedelta(days=1)
            tempend = tempend + timedelta(days=7)
        sourcefiles = os.listdir(os.getcwd())
        destinationpath = os.getcwd() + "/data/Partials/" + str(df['Client Id'].iloc[i])
        for fp in sourcefiles:
            if fp.endswith('.json'):
                shutil.move(os.path.join(os.getcwd(), fp), os.path.join(destinationpath, fp))
        sourcefiles = os.listdir(destinationpath)
        if (len(sourcefiles) > 0):
            datajs = []
            for jsn in sourcefiles:
                with open(os.path.join(destinationpath, jsn)) as json_data:
                    tempjs = json.load(json_data)
                for item in tempjs['dates']:
                    item['date'] = datetime.strptime(item['date'], '%b %d, %Y')
                    item['ClientID'] = str(df['Client Id'].iloc[i])
                    datajs.append(item)
            col.insert_many(datajs)
            shutil.rmtree(destinationpath)
    except WebDriverException:
        if (platform.system() == "Darwin"):
            driver = webdriver.Chrome(os.getcwd() + "/ChromeDrivers/chromedriver_mac", chrome_options=chrome_options)
        elif (platform.system() == "Linux"):
            driver = webdriver.Chrome(os.getcwd() + "/ChromeDrivers/chromedriver_linux", chrome_options=chrome_options)
        else:
            driver = webdriver.Chrome(os.getcwd() + "/ChromeDrivers/chromedriver.exe", chrome_options=chrome_options)

        # pull page ...login and get dataframe
        driver.get(
            "https://analytics.google.com/analytics/web/?authuser=1#/report/visitors-legacy-user-id/a177871w3490836p54583939/_u.date00=" + startd + "&_u.date01=" + endd)
        email_input = driver.find_element_by_tag_name('input')
        email_input.send_keys(params["gauser"])
        driver.find_element_by_id('identifierNext').click()
        time.sleep(30)
        pwd_input = driver.find_element_by_xpath('//*[@id="password"]/div[1]/div/div[1]/input')
        pwd_input.send_keys(params["gapwd"])
        driver.find_element_by_id('passwordNext').click()
        time.sleep(30)
        driver.switch_to.frame(driver.find_element_by_id("galaxyIframe"))
    except Exception as e:
        print(str(e))
        sourcefiles = os.listdir(os.getcwd())
        destinationpath = os.getcwd() + "/data/Partials/" + str(df['Client Id'].iloc[i])
        for fp in sourcefiles:
            if fp.endswith('.json'):
                os.remove(os.path.join(os.getcwd(), fp))
        shutil.rmtree(destinationpath)

# close driver
driver.quit()