# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_departments=spark.read.format('delta').table('silver.departments')
silver_departments.withColumn('refreshed_at',current_timestamp())\
    .filter(lower(col('is_quarantined'))=='FALSE').select('Dept_Id' ,'SRC_Dept_Id' ,'Name' ,'datasource').distinct()\
    .write.format('delta').mode('overwrite').saveAsTable('gold.dim_department')

row_count=silver_departments.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','dim_department',{row_count},current_timestamp())")