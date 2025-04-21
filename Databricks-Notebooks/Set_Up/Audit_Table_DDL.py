# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS audit.loads_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS audit.loads_logs (
# MAGIC   data_source STRING,
# MAGIC   tablename STRING,
# MAGIC   numberofrowscopied INT,
# MAGIC   watermarkcolumnname STRING,
# MAGIC   loaddate TIMESTAMP  
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit.loads_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table audit.loads_logs;