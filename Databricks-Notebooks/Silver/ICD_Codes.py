# Databricks notebook source
df = spark.read.parquet("/mnt/bronze/icd_codes")
df.createOrReplaceTempView("icd_codes")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.icd_codes(
# MAGIC   icd_code string,
# MAGIC   icd_code_type string,
# MAGIC   code_description string,
# MAGIC   inserted_date date,
# MAGIC   updated_date date,
# MAGIC   is_current_flag boolean
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into
# MAGIC   silver.icd_codes as target
# MAGIC using
# MAGIC   icd_codes as source
# MAGIC on 
# MAGIC   target.icd_code = source.icd_code 
# MAGIC when matched and (
# MAGIC   target.code_description != source.code_description
# MAGIC ) then
# MAGIC update set
# MAGIC   target.code_description = source.code_description,
# MAGIC   target.updated_date = source.updated_date,
# MAGIC   target.is_current_flag = False
# MAGIC when not matched then insert(
# MAGIC   icd_code,
# MAGIC   icd_code_type,
# MAGIC   code_description,
# MAGIC   inserted_date,
# MAGIC   updated_date,
# MAGIC   is_current_flag
# MAGIC ) values(
# MAGIC   source.icd_code,
# MAGIC   source.icd_code_type,
# MAGIC   source.code_description,
# MAGIC   source.inserted_date,
# MAGIC   source.updated_date,
# MAGIC   True
# MAGIC )
# MAGIC