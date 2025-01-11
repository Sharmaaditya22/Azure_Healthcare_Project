# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df=spark.read.csv("/mnt/landing/claims/*.csv",header=True)

df = df.withColumn(
    "datasource",
    when(input_file_name().contains("hospital1"), "hosa").when(input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

df.write.format("parquet").mode("overwrite").save("/mnt/bronze/claims/")

df=df.withColumn('SRC_ClaimID',col('ClaimID'))\
     .withColumn('ClaimID',concat(col('ClaimID'),lit('-'),col('datasource')))\
     .withColumn('ServiceDate',col('ServiceDate').cast('date'))\
     .withColumn('ClaimDate',col('ClaimDate').cast('date'))\
     .withColumn('SRC_InsertDate',col('InsertDate').cast('date'))\
     .withColumn('SRC_ModifiedDate',col('ModifiedDate').cast('date'))\
     .withColumn('is_quarantined',when((col('ClaimID').isNull()) | (col('TransactionID').isNull()) | (col('PatientID').isNull()) | (col('ServiceDate').isNull()) ,lit('TRUE')).otherwise(lit('FALSE')))\
     .select('ClaimID','SRC_ClaimID','TransactionID','PatientID','EncounterID','ProviderID','DeptID','ServiceDate','ClaimDate','PayorID','ClaimAmount','PaidAmount','ClaimStatus','PayorType','Deductible','Coinsurance','Copay','SRC_InsertDate','SRC_ModifiedDate','datasource','is_quarantined')

display(df)

silver_claim=DeltaTable.forName(spark,'silver.claims')
silver_claim.alias("target").merge(
    df.alias("source"),
    """
    target.ClaimID = source.ClaimID AND target.is_current = true
    """
).whenMatchedUpdate(
    condition="""
        target.SRC_ClaimID != source.SRC_ClaimID OR
        target.TransactionID != source.TransactionID OR
        target.PatientID != source.PatientID OR
        target.EncounterID != source.EncounterID OR
        target.ProviderID != source.ProviderID OR
        target.DeptID != source.DeptID OR
        target.ServiceDate != source.ServiceDate OR
        target.ClaimDate != source.ClaimDate OR
        target.PayorID != source.PayorID OR
        target.ClaimAmount != source.ClaimAmount OR
        target.PaidAmount != source.PaidAmount OR
        target.ClaimStatus != source.ClaimStatus OR
        target.PayorType != source.PayorType OR
        target.Deductible != source.Deductible OR
        target.Coinsurance != source.Coinsurance OR
        target.Copay != source.Copay OR
        target.SRC_InsertDate != source.SRC_InsertDate OR
        target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
        target.datasource != source.datasource OR
        target.is_quarantined != source.is_quarantined
    """,
    set={
        "is_current": "false",
        "audit_modifieddate": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "ClaimID": "source.ClaimID",
        "SRC_ClaimID": "source.SRC_ClaimID",
        "TransactionID": "source.TransactionID",
        "PatientID": "source.PatientID",
        "EncounterID": "source.EncounterID",
        "ProviderID": "source.ProviderID",
        "DeptID": "source.DeptID",
        "ServiceDate": "source.ServiceDate",
        "ClaimDate": "source.ClaimDate",
        "PayorID": "source.PayorID",
        "ClaimAmount": "source.ClaimAmount",
        "PaidAmount": "source.PaidAmount",
        "ClaimStatus": "source.ClaimStatus",
        "PayorType": "source.PayorType",
        "Deductible": "source.Deductible",
        "Coinsurance": "source.Coinsurance",
        "Copay": "source.Copay",
        "SRC_InsertDate": "source.SRC_InsertDate",
        "SRC_ModifiedDate": "source.SRC_ModifiedDate",
        "datasource": "source.datasource",
        "is_quarantined": "source.is_quarantined",
        "audit_insertdate": "current_timestamp()",
        "audit_modifieddate": "current_timestamp()",
        "is_current": "true"
    }
).execute()

row_count=df.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','claims',{row_count},current_timestamp())")
