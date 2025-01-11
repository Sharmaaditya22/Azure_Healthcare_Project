# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/departments")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/departments")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)

# Create the dept_id column and rename deptid to src_dept_id
df_merged = df_merged.withColumn("SRC_Dept_id", col("deptid")) \
                     .withColumn("Dept_id", concat(col("deptid"),lit('-'), col("datasource"))) \
                     .withColumn('is_quarantined',when((col('SRC_Dept_Id').isNull()) | (col('Name').isNull()),lit('TRUE')).otherwise(lit('FALSE')))\
                     .drop("deptid")
                     
df_merged.write.format('delta').mode('overwrite').saveAsTable('silver.departments')
#df_merged.createOrReplaceTempView("departments")

row_count=df_merged.count()
spark.sql(f"insert into audit.load_logs (data_source, tablename,numberofrowscopied, loaddate) values ('Bronze to Silver','departments',{row_count},current_timestamp())")
