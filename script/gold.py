#!/usr/local/bin/python3


###################################################################################################
#                                                                                                 #
# SCRIPT FILE: gold.py                                                                            #
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

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import re
import json
import glob
import sys
#variables



class Gold():
    def __init__(self,project, parameter) -> None:
    
        path = f"{project}/parameters/{parameter}.json"
   
        parameter=open(path)
        data = parameter.read()
        content = json.loads(data)
        spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()
       
        #recover
        silver_path = content["parquet_silver"]
        gold_path = content["parquet_gold"]


        df = spark.read.parquet(silver_path).createOrReplaceTempView("df")
        
        df = spark.sql("""SELECT
        id,
        first_name,       
        last_name,       
        cpf,       
        education,       
        server_agency,       
        unit_action_server,       
        uf_unit_action_server,       
        upag,       
        uf_upag,       
        office,       
        education_position,       
        linked_office,       
        original_office,       
        education_original_office,       
        situation,       
        level_gratification,       
        rubric,       
        value,       
        source,
        date_transform
        FROM df
        """)

        ##secund treatment
        df.write.mode("append").format("parquet").partitionBy("date_transform").save(f"{gold_path}gold")
        print(f"Parquet save in Datalake '{gold_path}' !")

