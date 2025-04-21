# Databricks notebook source
from pyspark.sql import functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/departments")
df_hosb = spark.read.parquet("/mnt/bronze/hosb/departments")

df_merged = df_hosa.unionByName(df_hosb)

df_merged = df_merged.withColumn("SRC_Dept_id",f.col("deptid")) \
                    .withColumn("Dept_id",f.concat(f.col("deptid"),f.lit("-"),f.col("datasource"))) \
                    .drop("deptid")

df_merged.createOrReplaceTempView("departments")

# df_merged.show()

# COMMAND ----------

temp = spark.sql("select * from departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments (
# MAGIC   Dept_id string,
# MAGIC   SRC_Dept_id string,
# MAGIC   Name string,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.departments;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.departments
# MAGIC select 
# MAGIC dept_id,
# MAGIC SRC_dept_id,
# MAGIC Name,
# MAGIC Datasource,
# MAGIC case
# MAGIC   when SRC_Dept_id is null or Name is null then true
# MAGIC   else false
# MAGIC end as is_quarantined  
# MAGIC from departments

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.departments;