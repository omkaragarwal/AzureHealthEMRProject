# Databricks notebook source
from pyspark.sql import functions as f

df = spark.read.parquet("/mnt/bronze/npi_extract")

df.createOrReplaceTempView("npi_extract")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from npi_extract limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.npi_extract(
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   last_updated string,
# MAGIC   npi_id string,
# MAGIC   organisation_name string,
# MAGIC   position string,
# MAGIC   refreshed_at date,
# MAGIC   inserted_date date,
# MAGIC   modified_date date,
# MAGIC   is_current boolean
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into
# MAGIC   silver.npi_extract as target
# MAGIC using
# MAGIC   npi_extract as source
# MAGIC on target.npi_id = source.npi_id and target.is_current = true
# MAGIC when matched and (
# MAGIC   target.refreshed_at != source.refreshed_at or
# MAGIC   target.first_name != source.first_name or
# MAGIC   target.last_name != source.last_name or
# MAGIC   target.last_updated != source.last_updated or
# MAGIC   target.position != source.position or
# MAGIC   target.organisation_name != source.organisation_name 
# MAGIC ) then update set 
# MAGIC   target.modified_date = current_date(),
# MAGIC   target.is_current = false
# MAGIC when not matched then insert (
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   last_updated,
# MAGIC   npi_id,
# MAGIC   organisation_name,
# MAGIC   position,
# MAGIC   refreshed_at,
# MAGIC   inserted_date,
# MAGIC   modified_date,
# MAGIC   is_current
# MAGIC )
# MAGIC values(
# MAGIC   source.first_name,
# MAGIC   source.last_name,
# MAGIC   source.last_updated,
# MAGIC   source.npi_id,
# MAGIC   source.organisation_name,
# MAGIC   source.position,
# MAGIC   source.refreshed_at,
# MAGIC   current_date(),
# MAGIC   current_date(),
# MAGIC   true
# MAGIC );
# MAGIC