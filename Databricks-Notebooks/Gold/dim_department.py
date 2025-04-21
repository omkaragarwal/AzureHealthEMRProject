# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.dim_dept(
# MAGIC   SRC_Dept_id string,
# MAGIC   Dept_id string,
# MAGIC   Name string,
# MAGIC   datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_dept;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_dept
# MAGIC select
# MAGIC   d.SRC_Dept_id,
# MAGIC   d.Dept_id,
# MAGIC   d.Name,
# MAGIC   d.datasource
# MAGIC from silver.departments d
# MAGIC where d.is_quarantined = false;