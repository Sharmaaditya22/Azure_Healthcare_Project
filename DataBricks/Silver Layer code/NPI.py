# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df=spark.read.parquet("/mnt/bronze/npi_extract")

display(df)

silver_cptcodes=DeltaTable.forName(spark,'silver.npi_extract')
silver_cptcodes.alias('target').merge(
  df.alias('source'),
  """
  target.npi_id = source.npi_id and target.is_current_flag = true
  """
).whenMatchedUpdate(
  condition="""
  target.first_name != source.first_name OR
  target.last_name != source.last_name OR
  target.position != source.position OR
  target.organisation_name != source.organisation_name OR
  target.last_updated != source.last_updated
  """,
  set={
  "target.updated_date":"current_date",
  "target.is_current_flag":"False"
  }
).whenNotMatchedInsert(
  values={
  "npi_id": "source.npi_id",
  "first_name": "source.first_name",
  "last_name": "source.last_name",
  "position": "source.position",
  "organisation_name": "source.organisation_name",
  "last_updated": "source.last_updated",
  "inserted_date": "current_date",
  "updated_date": "current_date",
  "is_current_flag": "true" 
  }
).execute()

row_count=df.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','npi_extract',{row_count},current_timestamp())")