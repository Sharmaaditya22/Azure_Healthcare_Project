# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/providers")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/providers")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)

df_merged=df_merged.withColumn('NPI',col('NPI').cast('int'))\
    .withColumn('is_quarantined',when((f.col('ProviderID').isNull()) | (col('DeptID').isNull()),lit('TRUE')).otherwise(lit('FALSE')))\
    .select('*').distinct()

display(df_merged)

df_merged.write.format('delta').mode('overwrite').saveAsTable('silver.providers')

row_count=df_merged.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','providers',{row_count},current_timestamp())")
