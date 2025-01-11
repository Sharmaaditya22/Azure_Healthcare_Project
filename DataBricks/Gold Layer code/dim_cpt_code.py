# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_cptcodes=spark.read.format('delta').table('silver.cptcodes')
silver_cptcodes.withColumn('refreshed_at',current_timestamp())\
    .filter((lower(col('is_quarantined'))=='false') & (col('is_current')=='true'))\
    .select('cpt_codes','procedure_code_category','procedure_code_descriptions','code_status','refreshed_at')\
    .write.format('delta').mode('overwrite').saveAsTable('gold.dim_cpt_code')

row_count=silver_cptcodes.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','dim_cpt_code',{row_count},current_timestamp())")