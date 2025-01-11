# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_patients=spark.read.format('delta').table('silver.patients')
silver_patients.filter((col('is_current')=='true') & (lower(col('is_quarantined'))=='false'))\
    .select('patient_key','src_patientid','firstname','lastname','middlename','ssn','phonenumber','gender','dob','address','datasource')\
    .write.format('delta').mode('overwrite').saveAsTable('gold.dim_patient')

row_count=silver_patients.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','dim_patient',{row_count},current_timestamp())")