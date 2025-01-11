# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_transactions = spark.read.format("delta").table("silver.transactions")
silver_transactions.withColumn("refreshed_at", current_timestamp())\
    .withColumn('FK_Patient_ID',concat(col('PatientID'),lit('-'),col('datasource')))\
    .withColumn('FK_Provider_ID',when(col('datasource')=='hos-a',concat(lit('H1-'),col('providerID'))).otherwise(concat(lit('H2-'),col('providerID'))))\
    .withColumn('FK_Dept_ID',concat(col('DeptID'),lit('-'),col('datasource')))\
    .filter((lower(col("is_current")) == "true") & (lower(col("is_quarantined")) == "false"))\
    .select('TransactionID','SRC_TransactionID','EncounterID','FK_Patient_ID','FK_Provider_ID','FK_Dept_ID','ICDCode',col('ProcedureCode').alias('CPT_Code'),'VisitType','ServiceDate','PaidDate',col('Amount').alias('Charge_Amt'),col('PaidAmount').alias('Paid_Amt'),'AmountType','ClaimID','datasource','refreshed_at')\
    .write.format('delta').mode('overwrite').saveAsTable('gold.fact_transactions')

row_count=silver_transactions.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Silver to Gold','fact_transactions',{row_count},current_timestamp())")