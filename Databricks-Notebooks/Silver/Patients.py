# Databricks notebook source
from pyspark.sql import functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/patients")
df_hosa.createOrReplaceTempView("patients_hosa")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/patients")
df_hosb.createOrReplaceTempView("patients_hosb")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view cdm_patients as
# MAGIC select concat(SRC_PatientID,'-',datasource) as PatientKey, *
# MAGIC from (
# MAGIC   SELECT
# MAGIC     PATientID as SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     ModifiedDate,
# MAGIC     datasource
# MAGIC   FROM patients_hosa
# MAGIC   UNION all
# MAGIC   select
# MAGIC     ID as SRC_PatientID,
# MAGIC     F_Name aS FirstName,
# MAGIC     L_Name as LastName,
# MAGIC     M_Name as MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     Updated_Date as ModifiedDate,
# MAGIC     datasource
# MAGIC   FROM patients_hosb
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdm_patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view quality_checks as 
# MAGIC select
# MAGIC   PatientKey,
# MAGIC   SRC_PatientID,
# MAGIC   FirstName,
# MAGIC   LastName,
# MAGIC   MiddleName,
# MAGIC   SSN,
# MAGIC   PhoneNumber,
# MAGIC   Gender,
# MAGIC   DOB,
# MAGIC   Address,
# MAGIC   ModifiedDate,
# MAGIC   datasource,
# MAGIC   case when SRC_PatientID is null or DOB is null or FirstName is null or lower(FirstName) = "null" then true else false end as is_quarantined
# MAGIC from cdm_patients

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quality_checks order by is_quarantined desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.patients(
# MAGIC   PatientKey string,
# MAGIC   SRC_PatientID string,
# MAGIC   FirstName string,
# MAGIC   LastName string,
# MAGIC   MiddleName string,
# MAGIC   SSN string,
# MAGIC   PhoneNumber string,
# MAGIC   Gender string,
# MAGIC   DOB date,
# MAGIC   Address string,
# MAGIC   SRC_ModifiedDate timestamp,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean,
# MAGIC   modifiedDate timestamp,
# MAGIC   insertedDate timestamp,
# MAGIC   is_current boolean
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.patients as target
# MAGIC using quality_checks as source
# MAGIC on target.PatientKey = source.PatientKey and target.is_current = true
# MAGIC when matched and 
# MAGIC (
# MAGIC   target.SRC_PatientID != source.SRC_PatientID or
# MAGIC   target.FirstName != source.FirstName or
# MAGIC   target.LastName != source.LastName or
# MAGIC   target.MiddleName != source.MiddleName or
# MAGIC   target.SSN != source.SSN or
# MAGIC   target.PhoneNumber != source.PhoneNumber or
# MAGIC   target.Gender != source.Gender or
# MAGIC   target.DOB != source.DOB or
# MAGIC   target.Address != source.Address or
# MAGIC   target.SRC_ModifiedDate != source.ModifiedDate or
# MAGIC   target.datasource != source.datasource or
# MAGIC   target.is_quarantined != source.is_quarantined
# MAGIC )
# MAGIC then update set
# MAGIC   target.is_current = false,
# MAGIC   target.modifiedDate = current_timestamp()
# MAGIC when not matched then insert
# MAGIC (
# MAGIC   PatientKey,
# MAGIC   SRC_PatientID,
# MAGIC   FirstName,
# MAGIC   LastName,
# MAGIC   MiddleName,
# MAGIC   SSN,
# MAGIC   PhoneNumber,
# MAGIC   Gender,
# MAGIC   DOB,
# MAGIC   Address,
# MAGIC   SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   insertedDate,
# MAGIC   modifiedDate,
# MAGIC   is_current
# MAGIC )
# MAGIC Values(
# MAGIC   source.PatientKey,
# MAGIC   source.SRC_PatientID,
# MAGIC   source.FirstName,
# MAGIC   source.LastName,
# MAGIC   source.MiddleName,
# MAGIC   source.SSN,
# MAGIC   source.PhoneNumber,
# MAGIC   source.Gender,
# MAGIC   source.DOB,
# MAGIC   source.Address,
# MAGIC   source.ModifiedDate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.patients as target
# MAGIC using quality_checks as source
# MAGIC on target.PatientKey = source.PatientKey and target.is_current = true
# MAGIC when not matched then insert
# MAGIC (
# MAGIC   PatientKey,
# MAGIC   SRC_PatientID,
# MAGIC   FirstName,
# MAGIC   LastName,
# MAGIC   MiddleName,
# MAGIC   SSN,
# MAGIC   PhoneNumber,
# MAGIC   Gender,
# MAGIC   DOB,
# MAGIC   Address,
# MAGIC   SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   insertedDate,
# MAGIC   modifiedDate,
# MAGIC   is_current
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC   source.PatientKey,
# MAGIC   source.SRC_PatientID,
# MAGIC   source.FirstName,
# MAGIC   source.LastName,
# MAGIC   source.MiddleName,
# MAGIC   source.SSN,
# MAGIC   source.PhoneNumber,
# MAGIC   source.Gender,
# MAGIC   source.DOB,
# MAGIC   source.Address,
# MAGIC   source.ModifiedDate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), PatientKey from silver.patients group by PatientKey order by count(*) desc