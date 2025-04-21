# Databricks notebook source
from pyspark.sql import functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/encounters")
df_hosa.createOrReplaceTempView("encounters_hosa")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/encounters")
df_hosb.createOrReplaceTempView("encounters_hosb")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view encounters as
# MAGIC (
# MAGIC   select * from encounters_hosa
# MAGIC   union all
# MAGIC   select * from encounters_hosb
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from encounters;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view quality_checks as
# MAGIC select
# MAGIC   concat(EncounterID,'-',datasource) as EncounterID,
# MAGIC   EncounterID as SRC_EncounterID,
# MAGIC   PatientID,
# MAGIC   EncounterDate,
# MAGIC   EncounterType,
# MAGIC   ProviderID,
# MAGIC   DepartmentID,
# MAGIC   ProcedureCode,
# MAGIC   InsertedDate as SRC_InsertedDate,
# MAGIC   ModifiedDate as SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   case
# MAGIC     when EncounterDate is Null or PatientId is null then true
# MAGIC     else false
# MAGIC   end as is_quarantined
# MAGIC   from encounters;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quality_checks where datasource = 'hos-b'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.encounters(
# MAGIC   EncounterID string,
# MAGIC   SRC_EncounterID string,
# MAGIC   PatientID string,
# MAGIC   EncounterDate date,
# MAGIC   EncounterType string,
# MAGIC   ProviderID string,
# MAGIC   DepartmentID string,
# MAGIC   ProcedureCode int,
# MAGIC   SRC_InsertedDate date,
# MAGIC   SRC_ModifiedDate date,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean,
# MAGIC   audit_insertdate timestamp,
# MAGIC   audit_modifieddate timestamp,
# MAGIC   is_current boolean
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.encounters as target
# MAGIC using quality_checks as source
# MAGIC on target.EncounterID = source.EncounterID and target.is_current = true
# MAGIC when matched and (
# MAGIC   target.is_quarantined <> source.is_quarantined or
# MAGIC   target.SRC_InsertedDate <> source.SRC_InsertedDate or
# MAGIC   target.SRC_ModifiedDate <> source.SRC_ModifiedDate or
# MAGIC   target.datasource <> source.datasource or
# MAGIC   target.PatientID <> source.PatientID or
# MAGIC   target.EncounterDate <> source.EncounterDate or
# MAGIC   target.EncounterType <> source.EncounterType or
# MAGIC   target.ProviderID <> source.ProviderID or
# MAGIC   target.DepartmentID <> source.DepartmentID or
# MAGIC   target.ProcedureCode <> source.ProcedureCode or
# MAGIC   target.SRC_EncounterID <> source.SRC_EncounterID
# MAGIC ) THEN 
# MAGIC   update set
# MAGIC   target.is_current = false,
# MAGIC   target.audit_modifieddate = current_timestamp()
# MAGIC when not matched then
# MAGIC insert(
# MAGIC   PatientID,
# MAGIC   EncounterDate,
# MAGIC   EncounterType,
# MAGIC   ProviderID,
# MAGIC   DepartmentID,
# MAGIC   ProcedureCode,
# MAGIC   SRC_InsertedDate,
# MAGIC   SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC ) values (
# MAGIC   source.PatientID,
# MAGIC   source.EncounterDate,
# MAGIC   source.EncounterType,
# MAGIC   source.ProviderID,
# MAGIC   source.DepartmentID,
# MAGIC   source.ProcedureCode,
# MAGIC   source.SRC_InsertedDate,
# MAGIC   source.SRC_ModifiedDate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select datasource,count(patientID) from silver.encounters group by all order by 3 desc;