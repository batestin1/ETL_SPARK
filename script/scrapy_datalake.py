#!/bin/bash

###################################################################################################
#                                                                                                 #
# SCRIPT FILE: scrapy_datalake.sh                                                                 #
# CREATION DATE: 08/11/2022                                                                       #
# HOUR: 09:25                                                                                     #
# DISTRIBUTION USED: UBUNTU                                                                       #
# OPERATIONAL SYSTEM: DEBIAN                                                                      #
#                                                                             DEVELOPED BY: BATES #
###################################################################################################
#                                                                                                 #
#SUMMARY: DATA ENGINEER ABOUT BR FEDERAL SERVERS GRATUITIES                                       #
#                                                                                                 #
###################################################################################################

# variables

import requests
from bs4 import BeautifulSoup
import re
import os
import json
from pathlib import Path
import glob
import sys

#variables




class Scrappy():
    def __init__(self, project, parameter) -> None:
       
        path = f"{project}/parameters/{parameter}.json"
        parameter=open(path)
        data = parameter.read()
        content = json.loads(data)

        #variables recover
        url_l = content["url"]
        csv_path = content["csv_path"]
        com = content["compile"]

        url = requests.get(url_l)
        soup = BeautifulSoup(url.content, features="html.parser")
        for i in soup.find_all(href=re.compile(com)):
            a = str(i)
            threat = a.replace('<a class="resource-url-analytics" href=', '').replace('rel="noopener noreferrer" target="_blank">', '').replace('<i class="icon-external-link"></i>', '').replace('            Ir para recurso', '').replace('        </a>', '').replace('\n','').replace('"','').replace('http://landpage-h.cgu.gov.br/dadosabertos/index.php?url=','').replace('           ','')
            response = requests.get(threat)
            if response.status_code == 200:
                arquivo_path = os.path.join(csv_path, os.path.basename(threat).lower())
                with open(arquivo_path, 'wb') as f:
                    f.write(response.content)
            
            else:
                print("Download failed")

        for file in glob.glob(f"{csv_path}*.pdf"):
            os.remove(file)

        print(f"Download succelly on '{csv_path}' !")

