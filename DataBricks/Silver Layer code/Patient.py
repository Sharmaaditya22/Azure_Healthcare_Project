# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

#Reading Hospital A patient data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/patients")
#Reading Hospital B patient data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/patients")

df_hosa=df_hosa.withColumnRenamed('PatientID','SRC_PatientID')
columns={'ID' :'SRC_PatientID',
    'F_Name':'FirstName',
    'L_Name': 'LastName',
    'M_Name': 'MiddleName','Updated_Date':'ModifiedDate'}

for old_col,new_col in columns.items():
    df_hosb=df_hosb.withColumnRenamed(old_col,new_col)

df_merged = df_hosa.unionByName(df_hosb)
df_merged=df_merged.withColumn('Patient_Key',concat(col('SRC_PatientID'),lit('-'),col('datasource')))\
        .selectExpr('Patient_Key','SRC_PatientID','FirstName','LastName','MiddleName','SSN','PhoneNumber','Gender','DOB','Address','ModifiedDate AS SRC_ModifiedDate','datasource')\
        .withColumn('is_quarantined',when((col('SRC_PatientID').isNull()) |(col('FirstName').isNull()) | (lower(col('FirstName'))=='null') , lit('TRUE')).otherwise(lit('FALSE')))

display(df_merged)

silver_patients=DeltaTable.forName(spark,'silver.patients')
silver_patients.alias('target').merge(
    df_merged.alias('source'),
    """
    target.Patient_Key = source.Patient_Key AND target.is_current = true 
    """
).whenMatchedUpdate(
    condition="""
    target.SRC_PatientID <> source.SRC_PatientID OR
    target.FirstName <> source.FirstName OR
    target.LastName <> source.LastName OR
    target.MiddleName <> source.MiddleName OR
    target.SSN <> source.SSN OR
    target.PhoneNumber <> source.PhoneNumber OR
    target.Gender <> source.Gender OR
    target.DOB <> source.DOB OR
    target.Address <> source.Address OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
    """,
    set={
    "target.is_current":"false",
    "target.modified_date":"current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
    "Patient_Key":"source.Patient_Key",
    "SRC_PatientID":"source.SRC_PatientID",
    "FirstName":"source.FirstName",
    "LastName":"source.LastName",
    "MiddleName":"source.MiddleName",
    "SSN":"source.SSN",
    "PhoneNumber":"source.PhoneNumber",
    "Gender":"source.Gender",
    "DOB":"source.DOB",
    "Address":"source.Address",
    "SRC_ModifiedDate":"source.SRC_ModifiedDate",
    "datasource":"source.datasource",
    "is_quarantined":"source.is_quarantined",
    "inserted_date":"current_timestamp()",
    "modified_date":"current_timestamp()",
    "is_current":"true"
    }
).execute()

row_count=df_merged.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','patients',{row_count},current_timestamp())")