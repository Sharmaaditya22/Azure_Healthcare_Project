# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_icd_codes=spark.read.format('delta').table('silver.icd_codes')
silver_icd_codes.withColumn('refreshed_at',current_timestamp())\
    .filter(col('is_current_flag')=='true')\
    .select('icd_code','icd_code_type','code_description','refreshed_at').distinct()\
    .write.format('delta').mode('overwrite').saveAsTable('gold.dim_icd')

row_count=silver_icd_codes.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','dim_icd',{row_count},current_timestamp())")