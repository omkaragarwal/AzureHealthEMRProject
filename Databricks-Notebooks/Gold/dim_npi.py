# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.dim_npi(
# MAGIC   npi_id string,
# MAGIC   FirstName string,
# MAGIC   LastName string,
# MAGIC   organisationName string,
# MAGIC   position string,
# MAGIC   last_updated string,
# MAGIC   refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_npi

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_npi
# MAGIC select 
# MAGIC   npi_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   organisation_name,
# MAGIC   position,
# MAGIC   last_updated,
# MAGIC   current_timestamp() as refreshed_at
# MAGIC from silver.npi_extract
# MAGIC where is_current = true