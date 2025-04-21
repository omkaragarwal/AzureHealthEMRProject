# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.dim_patients(
# MAGIC   PatientKey string,
# MAGIC   SRC_PatientID string,
# MAGIC   FirtsName string,
# MAGIC   LastName string,
# MAGIC   MiddleName string,
# MAGIC   SSN string,
# MAGIC   phoneNumber string,
# MAGIC   DOB date,
# MAGIC   Gender string,
# MAGIC   address string,
# MAGIC   datasource string
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_patients 
# MAGIC select 
# MAGIC   p.PatientKey,
# MAGIC   p.SRC_PatientID,
# MAGIC   p.FirstName,
# MAGIC   p.LastName,
# MAGIC   p.MiddleName,
# MAGIC   p.SSN,
# MAGIC   p.PhoneNumber,
# MAGIC   p.DOB,
# MAGIC   p.Gender,
# MAGIC   p.Address,
# MAGIC   p.datasource
# MAGIC from silver.patients p
# MAGIC where p.is_current = true and p.is_quarantined = false;