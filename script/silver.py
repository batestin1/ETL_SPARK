#!/usr/local/bin/python3

###################################################################################################
#                                                                                                 #
# SCRIPT FILE: silver.py                                                                          #
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



class Silver():
    def __init__(self, project, parameter) -> None:
        print(f"remove csv files ")
       

        project=sys.argv[1].replace(' ','_').lower()
        parameter=sys.argv[2].split('.')[0].lower()
        path = f"{project}/parameters/{parameter}.json"
        parameter=open(path)
        data = parameter.read()
        content = json.loads(data)
        spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()


        #recover
        csv_path = content["csv_path"]
        bronze_path = content["parquet_bronze"]
        silver_path = content["parquet_silver"]

        for file in glob.glob(f"{csv_path}*"):
            os.remove(file)


        df = spark.read.parquet(bronze_path).createOrReplaceTempView("df")

        #first treatment
        df = spark.sql("""SELECT
        monotonically_increasing_id() as id,
        initcap(lower(split(trim(NOME_SERVIDOR),' ')[0])) as first_name,
        initcap(lower(split(trim(NOME_SERVIDOR),' ',2)[1])) as last_name,
        IFNULL(replace(CPF, '*', '0'), 'null' ) as cpf,
        IFNULL(lower(replace(ESCOLARIDADE_SERVIDOR,' ','_')),'null') as education,
        IFNULL(lower(replace(ORGAO_EXERCICIO, ' ','_')),'null') as server_agency,
        IFNULL(lower(replace(UORG_EXERCICIO, ' ', '_')),'null') as unit_action_server,
        IFNULL(UF_UORG_EXERCICIO,'null') as uf_unit_action_server,
        IFNULL(lower(replace(UPAG,' ','_')),'null') as upag,
        IFNULL(UF_UPAG,'null') as uf_upag,
        IFNULL(lower(replace(CARGO,' ','_')),'null') as office,
        IFNULL(ESCOLARIDADE_CARGO,'null') as education_position,
        IFNULL(lower(replace(ORGAO_ORIGEM,' ','_')),'null') as linked_office,
        IFNULL(lower(replace(CARGO_ORIGEM,' ','_')),'null') as original_office,
        IFNULL(ESCOLARIDADE_CARGO_ORIGEM,'null') as education_original_office,
        IFNULL(SITUACAO_SERVIDOR,'null') as situation,
        IFNULL(NIVEL_GRATIFICACAO,'null') as level_gratification,
        IFNULL(lower(replace(replace(replace(replace(NOME_RUBRICA,'- LEI N�',''),'N�',''),'�6� ',''),'� 6�','')),'null') as rubric,
        IFNULL(replace(VALOR,',','.'),'null') as value,
        SOURCE as source,
        DATE_TRANSFORM as date_transform
        FROM df""").createOrReplaceTempView('df_a')

        ##secund treatment
        df = spark.sql(""" SELECT l. 
        * FROM (SELECT *, row_number() over 
        (partition by id order by date_transform desc) as row_id FROM df_a WHERE TRIM(id) <> '') l WHERE row_id = 1""")

        df.write.mode("append").format("parquet").partitionBy("date_transform").save(f"{silver_path}silver")

        print(f"Parquet save in Datalake '{silver_path}' !") 