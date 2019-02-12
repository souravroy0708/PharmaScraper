#custom scraper classes
from src.pharmarket import pharmarketean
from src.doctipharma import doctipharmaean
from src.astera import asteraean
from src.pharmabestamiens import pharmabestamiensean
from src.elsie import elsieean
from src.citypharma import citypharmaean
from src.mesoigner import mesoignerean
from src.mesoignerwithlogin import mesoignerwithloginean
from src.pharmavielogin import pharmavieloginean
from src.pharmaviewol import pharmaviewolean
from src.pharmacies1001 import pharmacies1001ean
# import libraries
import pandas as pd
import json

def main(config):
    eandf = pd.read_csv(config["eanlistfile"])
    configlist = []
    for template in config['eantemplates']:
        template['eanlist'] = eandf['Code Produit']
        configlist.append(template)
    threadlist = []
    for conf in configlist:
        exec(str("threadlist.append(" + conf["template"] + "(conf))"))
    for thread in threadlist:
        thread.start()
    for thread in threadlist:
        thread.join()


if __name__ == "__main__":
    with open('./conf/scrapconfig.json') as jsonfile:
        config = json.load(jsonfile)
    main(config)




