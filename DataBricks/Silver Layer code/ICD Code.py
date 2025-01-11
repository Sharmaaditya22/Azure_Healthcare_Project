# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

#Read ICD extracts from bronze layer
df=spark.read.format("parquet").load("/mnt/bronze/icd_codes/")

display(df)

silver_icd_codes=DeltaTable.forName(spark,'silver.icd_codes')
silver_icd_codes.alias('target').merge(
  df.alias('source'),
  """
  target.icd_code = source.icd_code
  """
).whenMatchedUpdate(
  condition="""
  target.code_description != source.code_description
  """,
  set={
  "target.code_description":"source.code_description",
  "target.updated_date":"source.updated_date",
  "target.is_current_flag":"False"
  }
).whenNotMatchedInsert(
  values={
  "icd_code":"source.icd_code",
  "icd_code_type":"source.icd_code_type",
  "code_description":"source.code_description",
  "inserted_date":"source.inserted_date",
  "updated_date":"source.updated_date",
  "is_current_flag":"source.is_current_flag"
  }
).execute()

row_count=df.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','icd_codes',{row_count},current_timestamp())")