# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/transactions")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/transactions")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)

df_merged=df_merged.withColumn('SRC_TransactionID',col('TransactionID'))\
                   .withColumn('TransactionID',concat(col('TransactionID'),lit('-'),col('datasource')))\
                   .withColumn('is_quarantined',when((col('EncounterID').isNull()) | (col('PatientID').isNull()) | (col('TransactionID').isNull()) | (col('VisitDate').isNull()),lit('TRUE')).otherwise(lit('FALSE')))\
                   .select('TransactionID','SRC_TransactionID','EncounterID','PatientID','ProviderID','DeptID','VisitDate','ServiceDate','PaidDate','VisitType','Amount','AmountType','PaidAmount','ClaimID','PayorID','ProcedureCode','ICDCode','LineOfBusiness','MedicaidID','MedicareID',col('InsertDate').alias('SRC_InsertDate'),col('ModifiedDate').alias('SRC_ModifiedDate'),'datasource','is_quarantined')

display(df_merged)

silver_transactions=DeltaTable.forName(spark,'silver.transactions')
silver_transactions.alias('target').merge(
  df_merged.alias('source'),
  """
  target.TransactionID = source.TransactionID AND target.is_current = true
  """
).whenMatchedUpdate(
  condition="""
  target.SRC_TransactionID != source.SRC_TransactionID
  OR target.EncounterID != source.EncounterID
  OR target.PatientID != source.PatientID
  OR target.ProviderID != source.ProviderID
  OR target.DeptID != source.DeptID
  OR target.VisitDate != source.VisitDate
  OR target.ServiceDate != source.ServiceDate
  OR target.PaidDate != source.PaidDate
  OR target.VisitType != source.VisitType
  OR target.Amount != source.Amount
  OR target.AmountType != source.AmountType
  OR target.PaidAmount != source.PaidAmount
  OR target.ClaimID != source.ClaimID
  OR target.PayorID != source.PayorID
  OR target.ProcedureCode != source.ProcedureCode
  OR target.ICDCode != source.ICDCode
  OR target.LineOfBusiness != source.LineOfBusiness
  OR target.MedicaidID != source.MedicaidID
  OR target.MedicareID != source.MedicareID
  OR target.SRC_InsertDate != source.SRC_InsertDate
  OR target.SRC_ModifiedDate != source.SRC_ModifiedDate
  OR target.datasource != source.datasource
  OR target.is_quarantined != source.is_quarantined
  """,
  set={
  "target.is_current":"false",
  "target.audit_modifieddate":"current_timestamp()"
  }
).whenNotMatchedInsert(
  values={
  "TransactionID":"source.TransactionID",
  "SRC_TransactionID":"source.SRC_TransactionID",
  "EncounterID":"source.EncounterID",
  "PatientID":"source.PatientID",
  "ProviderID":"source.ProviderID",
  "DeptID":"source.DeptID",
  "VisitDate":"source.VisitDate",
  "ServiceDate":"source.ServiceDate",
  "PaidDate":"source.PaidDate",
  "VisitType":"source.VisitDate",
  "Amount":"source.Amount",
  "AmountType":"source.AmountType",
  "PaidAmount":"source.PaidAmount",
  "ClaimID":"source.ClaimID",
  "PayorID":"source.PayorID",
  "ProcedureCode":"source.ProcedureCode",
  "ICDCode":"source.ICDCode",
  "LineOfBusiness":"source.LineOfBusiness",
  "MedicaidID":"source.MedicaidID",
  "MedicareID":"source.MedicareID",
  "SRC_InsertDate":"source.SRC_InsertDate",
  "SRC_ModifiedDate":"source.SRC_ModifiedDate",
  "datasource":"source.datasource",
  "is_quarantined":"source.is_quarantined",
  "audit_insertdate":"current_timestamp()",
  "audit_modifieddate":"current_timestamp()",
  "is_current":"true"
  }
).execute()

row_count=df_merged.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','transactions',{row_count},current_timestamp())")