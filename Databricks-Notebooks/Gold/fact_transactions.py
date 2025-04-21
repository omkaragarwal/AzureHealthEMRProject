# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.fact_transactions(
# MAGIC   TransactionID string,
# MAGIC   SRC_TransactionID string,
# MAGIC   EncounterID string,
# MAGIC   FK_PatientID string,
# MAGIC   FK_ProviderID string,
# MAGIC   FK_DeptID string,
# MAGIC   ICDCode string,
# MAGIC   ProcedureCode int,
# MAGIC   VisitType string,
# MAGIC   ServiceDate date,
# MAGIC   PaidDate date,
# MAGIC   Amount double,
# MAGIC   AmountType string,
# MAGIC   PaidAmount double,
# MAGIC   ClaimID string,
# MAGIC   datasource string,
# MAGIC   refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.fact_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.fact_transactions
# MAGIC select
# MAGIC   t.transactionid,
# MAGIC   t.src_transactionid,
# MAGIC   t.encounterid,
# MAGIC   concat(t.Patientid,'-',t.datasource) as fk_patientid,
# MAGIC   case when t.datasource = 'hos-a' then concat('H1-',t.providerid) else concat('H2-',t.providerid) end as fk_providerid,
# MAGIC   concat(t.deptid,'-',t.datasource) as fk_deptid,
# MAGIC   t.icdcode,
# MAGIC   t.procedurecode CPT_Code,
# MAGIC   t.visittype,
# MAGIC   t.servicedate,
# MAGIC   t.paiddate,
# MAGIC   t.amount Charge_Amt,
# MAGIC   t.amounttype,
# MAGIC   t.paidamount Paid_Amt,
# MAGIC   t.ClaimID,
# MAGIC   t.datasource,
# MAGIC   current_timestamp()
# MAGIC from silver.transactions t
# MAGIC where t.is_current = true and t.is_quarantined = false;