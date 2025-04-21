# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.provider(
# MAGIC   provider_id string,
# MAGIC   FirstName string,
# MAGIC   LastName string,
# MAGIC   dept_id string,
# MAGIC   npi bigint,
# MAGIC   datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.provider

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.provider
# MAGIC select
# MAGIC   p.providerid,
# MAGIC   p.firstname,
# MAGIC   p.lastname,
# MAGIC   p.deptid,
# MAGIC   p.npi,
# MAGIC   p.datasource
# MAGIC from silver.providers p
# MAGIC where p.is_quarantined = false;