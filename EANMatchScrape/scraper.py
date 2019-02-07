#custom scraper classes

# import libraries
import random
import pandas as pd
import json
import threading
import argparse

def main(config,nthread):
    eandf = pd.read_csv(config["eanlistfile"])
    for i in range(0,len(eandf)):
        ean = eandf['Code Produit'][i]
        for template in config['eantemplates']:
            template['ean'] = ean
            exec(str("threadlist.append(" + template["template"] + "(template))"))



if __name__ == "__main__":
    with open('./conf/scrapconfig.json') as jsonfile:
        config = json.load(jsonfile)
    parser = argparse.ArgumentParser()
    parser.add_argument('--whichtemplate', dest='whichtemplate', default="pharmanity",help='mention the class name to run')
    parser.add_argument('--numthreads', dest='numthreads', default=10, help='how many threads')
    args = parser.parse_args()
    main(config,int(args.numthreads))




