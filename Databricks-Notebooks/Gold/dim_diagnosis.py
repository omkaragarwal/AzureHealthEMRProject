# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.icd_codes(
# MAGIC   icd_code string,
# MAGIC   code_description string,
# MAGIC   ics_code_type string,
# MAGIC   refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.icd_codes;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.icd_codes
# MAGIC select 
# MAGIC   i.icd_code,
# MAGIC   i.code_description,
# MAGIC   i.icd_code_type,
# MAGIC   current_timestamp() as refreshed_at
# MAGIC from silver.icd_codes i
# MAGIC where i.is_current_flag = true;