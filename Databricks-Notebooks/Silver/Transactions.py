# Databricks notebook source
from pyspark.sql import functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/transactions")
df_hosb = spark.read.parquet("/mnt/bronze/hosb/transactions")

df_merged = df_hosa.unionByName(df_hosb)

df_merged.createOrReplaceTempView("transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view quality_checks as
# MAGIC select 
# MAGIC   concat(TransactionID,'-',datasource) as TransactionID,
# MAGIC   TransactionID as SRC_TransactionID,
# MAGIC   EncounterID,
# MAGIC   PatientID,
# MAGIC   ProviderID,
# MAGIC   DeptID,
# MAGIC   VisitDate,
# MAGIC   ServiceDate,
# MAGIC   PaidDate,
# MAGIC   VisitType,
# MAGIC   Amount,
# MAGIC   AmountType,
# MAGIC   PaidAmount,
# MAGIC   ClaimID,
# MAGIC   PayorID,
# MAGIC   ProcedureCode,
# MAGIC   ICDCode,
# MAGIC   LineOfBusiness,
# MAGIC   MedicaidID,
# MAGIC   MedicareID,
# MAGIC   InsertDate,
# MAGIC   ModifiedDate,
# MAGIC   datasource,
# MAGIC   case
# MAGIC     when EncounterID is null OR PatientID is null OR ProviderID is null or TransactionID is null then true
# MAGIC     else false
# MAGIC   end as is_quarantined
# MAGIC   from transactions;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.transactions
# MAGIC (
# MAGIC   TransactionID string,
# MAGIC   SRC_TransactionID string,
# MAGIC   EncounterID string,
# MAGIC   PatientID string,
# MAGIC   ProviderID string,
# MAGIC   DeptID string,
# MAGIC   VisitDate date,
# MAGIC   ServiceDate date,
# MAGIC   PaidDate date,
# MAGIC   VisitType string,
# MAGIC   Amount double,
# MAGIC   AmountType string,
# MAGIC   PaidAmount double,
# MAGIC   ClaimID string,
# MAGIC   PayorID string,
# MAGIC   ProcedureCode integer,
# MAGIC   ICDCode string,
# MAGIC   LineOfBusiness string,
# MAGIC   MedicaidID string,
# MAGIC   MedicareID string,
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
# MAGIC merge into silver.transactions as target
# MAGIC using quality_checks as source
# MAGIC on target.TransactionID = source.TransactionID and target.is_current = true
# MAGIC when matched and(
# MAGIC   target.SRC_TransactionID <> source.SRC_TransactionID OR
# MAGIC   target.EncounterID <> source.EncounterID OR
# MAGIC   target.PatientID <> source.PatientID OR
# MAGIC   target.ProviderID <> source.ProviderID OR
# MAGIC   target.DeptID <> source.DeptID OR
# MAGIC   target.VisitDate <> source.VisitDate OR
# MAGIC   target.ServiceDate <> source.ServiceDate OR
# MAGIC   target.PaidDate <> source.PaidDate OR
# MAGIC   target.VisitType <> source.VisitType OR
# MAGIC   target.Amount <> source.Amount OR
# MAGIC   target.AmountType <> source.AmountType OR
# MAGIC   target.PaidAmount <> source.PaidAmount OR
# MAGIC   target.ClaimID <> source.ClaimID OR
# MAGIC   target.PayorID <> source.PayorID OR
# MAGIC   target.ProcedureCode <> source.ProcedureCode OR
# MAGIC   target.ICDCode <> source.ICDCode OR
# MAGIC   target.LineOfBusiness <> source.LineOfBusiness OR
# MAGIC   target.MedicaidID <> source.MedicaidID OR
# MAGIC   target.MedicareID <> source.MedicareID OR
# MAGIC   target.SRC_InsertDate <> source.InsertDate OR
# MAGIC   target.SRC_ModifiedDate <> source.ModifiedDate OR
# MAGIC   target.datasource <> source.datasource OR
# MAGIC   target.is_quarantined <> source.is_quarantined
# MAGIC ) then
# MAGIC   update set 
# MAGIC   target.is_current = false,
# MAGIC   target.audit_modifieddate = current_timestamp()
# MAGIC when not matched then
# MAGIC   insert (
# MAGIC     TransactionID,
# MAGIC     SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     SRC_InsertDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate, 
# MAGIC     is_current
# MAGIC   ) Values
# MAGIC   (
# MAGIC     source.TransactionID,
# MAGIC     source.SRC_TransactionID,
# MAGIC     source.EncounterID,
# MAGIC     source.PatientID,
# MAGIC     source.ProviderID,
# MAGIC     source.DeptID,
# MAGIC     source.VisitDate,
# MAGIC     source.ServiceDate,
# MAGIC     source.PaidDate,
# MAGIC     source.VisitType,
# MAGIC     source.Amount,
# MAGIC     source.AmountType,
# MAGIC     source.PaidAmount,
# MAGIC     source.ClaimID,
# MAGIC     source.PayorID,
# MAGIC     source.ProcedureCode,
# MAGIC     source.ICDCode,
# MAGIC     source.LineOfBusiness,
# MAGIC     source.MedicaidID,
# MAGIC     source.MedicareID,
# MAGIC     source.InsertDate,
# MAGIC     source.ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true
# MAGIC   );