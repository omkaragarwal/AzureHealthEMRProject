# Databricks notebook source
from pyspark.sql import functions as f

df = spark.read.parquet("/mnt/bronze/claims")

df.createOrReplaceTempView("claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view quality_checks as
# MAGIC select 
# MAGIC   concat(ClaimID,"-",datasource) as ClaimID,
# MAGIC   ClaimId as SRC_ClaimId,
# MAGIC   TransactionID,
# MAGIC   PatientID,
# MAGIC   EncounterID,
# MAGIC   ProviderID,
# MAGIC   DeptId,
# MAGIC   ServiceDate,
# MAGIC   ClaimDate,
# MAGIC   PayorID,
# MAGIC   ClaimAmount,
# MAGIC   PaidAmount,
# MAGIC   ClaimStatus,
# MAGIC   PayorType,
# MAGIC   Deductible,
# MAGIC   Coinsurance,
# MAGIC   Copay,
# MAGIC   cast(InsertDate as date) as SRC_InsertDate,
# MAGIC   cast(ModifiedDate as date) as SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   case
# MAGIC     when ClaimId is Null or TransactionID is null or PatientID is null or ServiceDate is null then true
# MAGIC     else false
# MAGIC   end as is_quarantined
# MAGIC from claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quality_checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.Claims(
# MAGIC   ClaimId string,
# MAGIC   SRC_ClaimId string,
# MAGIC   TransactionID string,
# MAGIC   PatientID string,
# MAGIC   EncounterID string,
# MAGIC   ProviderID string,
# MAGIC   DeptId string,
# MAGIC   ServiceDate date,
# MAGIC   ClaimDate date,
# MAGIC   payorID string,
# MAGIC   ClaimAmount string,
# MAGIC   PaidAmount string,
# MAGIC   ClaimStatus string,
# MAGIC   PayorType string,
# MAGIC   deductible string,
# MAGIC   Coinsurance string,
# MAGIC   Copay string,
# MAGIC   SRC_InsertDate date,
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
# MAGIC merge into silver.claims as target
# MAGIC using quality_checks as source
# MAGIC on target.ClaimId = source.ClaimId and is_current = true
# MAGIC when matched and (
# MAGIC   target.SRC_ClaimId <> source.SRC_ClaimId or
# MAGIC   target.TransactionID <> source.TransactionID or
# MAGIC   target.PatientID <> source.PatientID or
# MAGIC   target.EncounterID <> source.EncounterID or
# MAGIC   target.ProviderID <> source.ProviderID or
# MAGIC   target.DeptId <> source.DeptId or
# MAGIC   target.ServiceDate <> source.ServiceDate or
# MAGIC   target.ClaimDate <> source.ClaimDate or
# MAGIC   target.payorID <> source.PayorID or
# MAGIC   target.ClaimAmount <> source.ClaimAmount or
# MAGIC   target.PaidAmount <> source.PaidAmount or
# MAGIC   target.ClaimStatus <> source.ClaimStatus or
# MAGIC   target.PayorType <> source.PayorType or
# MAGIC   target.deductible <> source.deductible or
# MAGIC   target.Coinsurance <> source.Coinsurance or
# MAGIC   target.Copay <> source.Copay or
# MAGIC   target.SRC_InsertDate <> source.SRC_InsertDate or
# MAGIC   target.SRC_ModifiedDate <> source.SRC_ModifiedDate or
# MAGIC   target.datasource <> source.datasource or
# MAGIC   target.is_quarantined <> source.is_quarantined
# MAGIC ) then update set 
# MAGIC   target.audit_modifieddate = current_timestamp(), 
# MAGIC   target.is_current = false
# MAGIC when not matched then insert (
# MAGIC   ClaimId,
# MAGIC   SRC_ClaimId,
# MAGIC   TransactionID,
# MAGIC   PatientID,
# MAGIC   EncounterID,
# MAGIC   ProviderID,
# MAGIC   DeptId,
# MAGIC   ServiceDate,
# MAGIC   ClaimDate,
# MAGIC   payorID,
# MAGIC   ClaimAmount,
# MAGIC   PaidAmount,
# MAGIC   ClaimStatus,
# MAGIC   PayorType,
# MAGIC   deductible,
# MAGIC   Coinsurance,
# MAGIC   Copay,
# MAGIC   SRC_InsertDate,
# MAGIC   SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC ) values (
# MAGIC   source.ClaimId,
# MAGIC   source.SRC_ClaimId,
# MAGIC   source.TransactionID,
# MAGIC   source.PatientID,
# MAGIC   source.EncounterID,
# MAGIC   source.ProviderID,
# MAGIC   source.DeptId,
# MAGIC   source.ServiceDate,
# MAGIC   source.ClaimDate,
# MAGIC   source.PayorID,
# MAGIC   source.ClaimAmount,
# MAGIC   source.PaidAmount,
# MAGIC   source.ClaimStatus,
# MAGIC   source.PayorType,
# MAGIC   source.deductible,
# MAGIC   source.Coinsurance,
# MAGIC   source.Copay,
# MAGIC   source.SRC_InsertDate,
# MAGIC   source.SRC_ModifiedDate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   true
# MAGIC );