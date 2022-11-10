#!/usr/local/bin/python3

###################################################################################################
#                                                                                                 #
# SCRIPT FILE: bronze.py                                                                          #
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

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import re
import json
import glob
import sys


class Bronze():
    def __init__(self, project, parameter) -> None:

        path = f"{project}/parameters/{parameter}.json"
        parameter=open(path)
        data = parameter.read()
        content = json.loads(data)
        spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()


        #recover
        csv_path = content["csv_path"]
        bronze_path = content["parquet_bronze"]

        for file in glob.glob(f"{csv_path}/*"):
            name=os.path.basename(file).replace('gratificacoes_','').replace('.csv','')
            df = spark.read.option("delimiter", ';').option("header", "true").csv(file)
            df.createOrReplaceTempView(f"df_{name}")
            df = spark.sql(f"""SELECT *, {name} AS SOURCE, DATE_FORMAT(current_date(),'yyyyMMdd') AS DATE_TRANSFORM 
            FROM df_{name} where SITUACAO_SERVIDOR = 'ATIVO PERMANENTE'""")

            #load process
            df.write.mode("append").format("parquet").partitionBy("DATE_TRANSFORM").save(f"{bronze_path}bronze")

        print(f"Parquet save in Datalake '{bronze_path}' !")