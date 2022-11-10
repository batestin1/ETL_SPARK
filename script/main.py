#!/usr/local/bin/python3

###################################################################################################
#                                                                                                 #
# SCRIPT FILE: main.py                                                                            #
# CREATION DATE: 08/11/2022                                                                       #
# HOUR: 09:25                                                                                     #
# DISTRIBUTION USED: UBUNTU                                                                       #
# OPERATIONAL SYSTEM: DEBIAN                                                                      #
#                                                                             DEVELOPED BY: BATES #
###################################################################################################
#                                                                                                 #
# SUMMARY: DATA ENGINEER ABOUT BR FEDERAL SERVERS GRATUITIES                                      #
#                                                                                                 #
###################################################################################################

# variables

from scrapy_datalake import Scrappy
from bronze import Bronze
from silver import Silver
from gold import Gold
import os
import json
import sys

 
project= sys.argv[1].replace(' ','_').lower()
parameter= sys.argv[2].split('.')[0].lower()
path = f"{project}/parameters/{parameter}.json"
   
param=open(path)
data = param.read()
content = json.loads(data)

if __name__ == '__main__':
    if Scrappy(project, parameter):
         if Bronze(project, parameter):
            if Silver(project, parameter):
                 Gold(project, parameter)

