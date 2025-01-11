# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, lit, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Read the CSV file
df = spark.read.csv("/mnt/landing/cptcodes/*.csv", header=True)

# Replace whitespaces in column names with underscores and convert to lowercase
for col_name in df.columns:
    new_col = col_name.replace(" ", "_").lower()
    df = df.withColumnRenamed(col_name, new_col)

df.write.format("parquet").mode("overwrite").save("/mnt/bronze/cpt_codes")

df = df.withColumn(
    'is_quarantined',
    when(
        (col('cpt_codes').isNull()) | (col('procedure_code_descriptions').isNull()),
        lit('True')
    ).otherwise(lit('False'))
).select(
    'cpt_codes', 'procedure_code_category', 'procedure_code_descriptions', 'code_status', 'is_quarantined'
)

display(df)

silver_cptcodes = DeltaTable.forName(spark, 'silver.cptcodes')
silver_cptcodes.alias('target').merge(
    df.alias('source'),
    """
    target.cpt_codes = source.cpt_codes AND target.is_current = true
    """
).whenMatchedUpdate(
    condition="""
    target.procedure_code_category != source.procedure_code_category OR
    target.procedure_code_descriptions != source.procedure_code_descriptions OR
    target.code_status != source.code_status OR
    target.is_quarantined != source.is_quarantined
    """,
    set={
        "is_current": "false",
        "audit_modifieddate": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "cpt_codes": "source.cpt_codes",
        "procedure_code_category": "source.procedure_code_category",
        "procedure_code_descriptions": "source.procedure_code_descriptions",
        "code_status": "source.code_status",
        "is_quarantined": "source.is_quarantined",
        "audit_insertdate": "current_timestamp()",
        "audit_modifieddate": "current_timestamp()",
        "is_current": "true"
    }
).execute()

row_count = df.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename, numberofrowscopied, loaddate) values ('Bronze to Silver', 'cptcodes', {row_count}, current_timestamp())")