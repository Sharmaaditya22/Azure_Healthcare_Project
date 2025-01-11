# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists audit.load_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create database audit

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists audit

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS audit.load_logs (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     data_source STRING,
# MAGIC     tablename STRING,
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname STRING,
# MAGIC     loaddate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table audit.load_logs 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from audit.load_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC select coalesce(cast(max(loaddate) as date),'1900-01-01') as last_fetched_date from audit.load_logs where data_source='hos-a' and tablename='dbo.encounters'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY audit.load_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE audit.load_logs;
# MAGIC