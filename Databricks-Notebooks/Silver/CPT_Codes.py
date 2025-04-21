# Databricks notebook source
from pyspark.sql import functions as f

df = spark.read.parquet("/mnt/bronze/cpt_codes")
df.createOrReplaceTempView("cpt_codes")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cpt_codes;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view quality_checks as
# MAGIC select
# MAGIC   cpt_codes,
# MAGIC   procedure_code_descriptions,
# MAGIC   procedure_code_category,
# MAGIC   code_status,
# MAGIC   case
# MAGIC     when cpt_codes is null or procedure_code_descriptions is null then true
# MAGIC     else false
# MAGIC   end as is_quarantined 
# MAGIC from cpt_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quality_checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.cptcodes(
# MAGIC   cpt_codes string,
# MAGIC   procedure_code_descriptions string,
# MAGIC   procedure_code_category string,
# MAGIC   code_status string,
# MAGIC   is_quarantined boolean,
# MAGIC   audit_insertdate timestamp,
# MAGIC   audit_modifieddate timestamp,
# MAGIC   is_current boolean
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.cptcodes as target
# MAGIC using quality_checks as source
# MAGIC on target.cpt_codes = source.cpt_codes and target.is_current = true
# MAGIC when matched and (
# MAGIC   target.procedure_code_descriptions != source.procedure_code_descriptions or
# MAGIC   target.procedure_code_category != source.procedure_code_category or
# MAGIC   target.code_status != source.code_status or
# MAGIC   target.is_quarantined != source.is_quarantined
# MAGIC ) then
# MAGIC update set
# MAGIC   target.is_current = false,
# MAGIC   target.audit_modifieddate = current_timestamp()
# MAGIC when not matched then insert(
# MAGIC   cpt_codes,
# MAGIC   procedure_code_descriptions,
# MAGIC   procedure_code_category,
# MAGIC   code_status,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC ) Values (
# MAGIC   source.cpt_codes,
# MAGIC   source.procedure_code_descriptions,
# MAGIC   source.procedure_code_category,
# MAGIC   source.code_status,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   true
# MAGIC );