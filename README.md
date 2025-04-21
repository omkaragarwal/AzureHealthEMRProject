Azure Healthcare Data Engineering End-to-End Project
This project demonstrates a real-world Healthcare Data Engineering pipeline built using Azure services and follows a Lakehouse architecture (Bronze, Silver, Gold). It involves processing and modeling data from two hospitals, enabling powerful healthcare analytics and insights.

🔧 Tech Stack
Azure Data Factory (ADF) – Data ingestion from Azure SQL DB & REST API

Azure Data Lake Storage Gen2 (ADLS Gen2) – Data lake for staging and storage

Azure Databricks (Unity Catalog + Delta Lake) – Data cleaning, transformation & modeling

Azure Key Vault – Secure credential management

Parquet Format – Efficient columnar data storage

Delta Tables – Versioned tables for audit/history (Silver & Gold layer)

📁 Data Sources
We’re working with two hospitals, each contributing the following five tables:

Patients – Patient master details

Departments – Department information

Providers – Doctor/Healthcare provider info

Encounters – Day-to-day hospital visits, admissions, procedures

Transactions – Every financial transaction made by patients

Additional Data Sources:
claims.csv – Dropped by insurance companies in the landing folder of ADLS Gen2

ICD Codes API – Provides diagnosis code info

NPI Dataset API – Provides provider (doctor) registry info

CPT Codes File – In the landing folder; contains procedure details for treatments

🚀 Pipeline Overview
📥 Ingestion – Azure Data Factory
Created metadata-driven ADF pipelines to:

Extract structured data from Azure SQL DB

Fetch ICD and NPI data using REST API calls

Ingest claims and CPT code CSVs from the landing folder

All raw data is stored in Parquet format in the Bronze layer of ADLS Gen2

🥉 Bronze Layer – Raw Zone
All ingested data (SQL, APIs, CSVs) is saved in Parquet format

Schema variations are preserved (especially for patient data across hospitals)

No transformations are done in this stage

🥈 Silver Layer – Cleaned & Modeled Zone
Applied Common Data Model (CDM) to normalize schema differences across the two hospitals (especially in patients table)

Implemented Slowly Changing Dimensions (SCD) Type 2 on all core datasets to track historical changes

Cleaned and transformed data using Azure Databricks

Stored output in Delta Tables under Unity Catalog for consistency and governance

🥇 Gold Layer – Business Insights Zone
Consumed cleaned and modeled data from the Silver layer

Created fact and dimension tables optimized for reporting and dashboarding

Provides business users with clear insights for decision-making

💡 Claims & Financial Analysis
Claims dataset helps distinguish:

Fully covered patients (insurance pays 100%)

Co-pay patients (shared responsibility between patient & insurer)

Analyzed Amount Received metrics to estimate payment timelines:

For example, if the average payment time is >90 days, it's likely the hospital will eventually be paid

This helps identify revenue risks and manage hospital cash flow better

🔐 Security & Secrets Management
Used Azure Key Vault to securely store:

SQL Database credentials

ADLS Gen2 access keys

Azure Databricks workspace tokens

📊 Impact
Enabled centralized data processing across multiple hospital systems

Created scalable, governed, and insight-ready datasets

Supports clinical insights, financial analysis, and operational efficiency
