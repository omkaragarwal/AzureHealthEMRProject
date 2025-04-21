# Databricks notebook source
from pyspark.sql import functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/providers")
df_hosb = spark.read.parquet("/mnt/bronze/hosb/providers")

df_merge = df_hosa.unionByName(df_hosb)

df_merge.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists silver.providers
# MAGIC (ProviderID string,
# MAGIC  FirstName string,
# MAGIC  LastName string,
# MAGIC  Specialization string,
# MAGIC  DeptID string,
# MAGIC  NPI long,
# MAGIC  datasource string,
# MAGIC is_quarantined boolean
# MAGIC  )
# MAGIC  using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.providers;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into silver.providers
# MAGIC select 
# MAGIC   ProviderID,
# MAGIC    FirstName,
# MAGIC    LastName,
# MAGIC    Specialization,
# MAGIC    DeptID,
# MAGIC    NPI,
# MAGIC    datasource,
# MAGIC    CASE 
# MAGIC     when ProviderID is null or FirstName is null then true
# MAGIC     else false
# MAGIC   end as is_quarantined
# MAGIC from providers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.providers;