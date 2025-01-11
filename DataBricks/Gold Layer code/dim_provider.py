# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_providers=spark.read.format('delta').table('silver.providers')
silver_providers.withColumn('deptid',concat(col('DeptID'),lit('-'),col('datasource')))\
    .filter(lower(col('is_quarantined'))=='false')\
    .select('ProviderID','FirstName','LastName','deptid','NPI','datasource')\
    .write.format('delta').mode('overwrite').saveAsTable('gold.dim_provider')

row_count=silver_providers.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','dim_provider',{row_count},current_timestamp())")