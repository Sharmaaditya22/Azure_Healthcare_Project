# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_npi_extract=spark.read.format('delta').table('silver.npi_extract')
silver_npi_extract.withColumn('refreshed_at',current_timestamp())\
    .filter(col('is_current_flag')=='true')\
    .select('npi_id','first_name','last_name','position','organisation_name','last_updated','refreshed_at')\
    .write.format('delta').mode('overwrite').saveAsTable('gold.dim_npi')

row_count=silver_npi_extract.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','dim_npi',{row_count},current_timestamp())")