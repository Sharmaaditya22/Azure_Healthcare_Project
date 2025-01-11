# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

hosa_encounters=spark.read.parquet('/mnt/bronze/hosa/encounters')
hosa_encounters=spark.read.parquet('/mnt/bronze/hosb/encounters')

df_merged = hosa_encounters.unionByName(hosa_encounters)

df_merged=df_merged.withColumn('SRC_EncounterID',col('EncounterID'))\
                   .withColumn('EncounterID',concat(col('EncounterID'),lit('-'),col('datasource')))\
                   .withColumn('is_quarantined',when((col('EncounterID').isNull()) | (col('PatientID').isNull()),lit('TRUE')).otherwise(lit('FALSE')))\
                   .select('EncounterID','SRC_EncounterID','PatientID','EncounterDate','EncounterType','ProviderID','DepartmentID','ProcedureCode',col('InsertedDate').alias('SRC_InsertedDate'),col('ModifiedDate').alias('SRC_ModifiedDate'),'datasource','is_quarantined')

display(df_merged)

silver_encounters=DeltaTable.forName(spark,'silver.encounters')

silver_encounters.alias('target').merge(
    df_merged.alias('source'),
    """
    target.EncounterID = source.EncounterID AND target.is_current = true
    """
).whenMatchedUpdate(
    condition="""
    target.SRC_EncounterID != source.SRC_EncounterID OR
    target.PatientID != source.PatientID OR
    target.EncounterDate != source.EncounterDate OR
    target.EncounterType != source.EncounterType OR
    target.ProviderID != source.ProviderID OR
    target.DepartmentID != source.DepartmentID OR
    target.ProcedureCode != source.ProcedureCode OR
    target.SRC_InsertedDate != source.SRC_InsertedDate OR
    target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
    target.datasource != source.datasource OR
    target.is_quarantined != source.is_quarantined
    """,
    set={
    "target.is_current" : "false",
    "target.audit_modifieddate" : "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
    "EncounterID":"source.EncounterID",
    "SRC_EncounterID":"source.SRC_EncounterID",
    "PatientID":"source.PatientID",
    "EncounterDate":"source.EncounterDate",
    "EncounterType":"source.EncounterType",
    "ProviderID":"source.ProviderID",
    "DepartmentID":"source.DepartmentID",
    "ProcedureCode":"source.ProcedureCode",
    "SRC_InsertedDate":"source.SRC_InsertedDate",
    "SRC_ModifiedDate":"source.SRC_ModifiedDate",
    "datasource":"source.is_quarantined",
    "is_quarantined":"source.is_quarantined",
    "audit_insertdate":"current_timestamp()",
    "audit_modifieddate":"current_timestamp()",
    "is_current":"true"
    }
).execute()

row_count=df_merged.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','encounters',{row_count},current_timestamp())")