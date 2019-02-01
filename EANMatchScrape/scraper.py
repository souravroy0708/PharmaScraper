#custom scraper classes

# import libraries
import random
import pymongo
import pandas as pd
import json
import threading
import argparse

def main(config,nthread):
    eandf = pd.read_csv(config["eanlistfile"])



if __name__ == "__main__":
    with open('./conf/scrapconfig.json') as jsonfile:
        config = json.load(jsonfile)
    parser = argparse.ArgumentParser()
    parser.add_argument('--whichtemplate', dest='whichtemplate', default="lasante",help='mention the class name to run')
    parser.add_argument('--numthreads', dest='numthreads', default=10, help='how many threads')
    main(config,int(args.numthreads))


    args = parser.parse_args()
    if (args.whichtemplate != "all"):
        confdict=[]
        for item in scrapconfig:
            if (item['template'] == args.whichtemplate):
                confdict.append(item)
            else:
                continue
        scrapconfig = confdict
    main(scrapconfig,int(args.numthreads))



