Project Title: Azure Data Engineering End-to-End Pipeline (EMR Project)

This project involved building a complete data engineering solution using various Azure services. I utilized Azure Data Factory (ADF) to design and implement metadata-driven pipelines that extracted data from Azure SQL Database and loaded it into Azure Data Lake Storage Gen2 (ADLS Gen2).

In the Bronze layer, I stored the raw data in Parquet format, which provides efficient data compression and faster query performance.

Moving to the Silver layer, I focused on data cleaning, transformation, and standardization. I implemented Common Data Modeling for consistency across different tables and applied Slowly Changing Dimension (SCD) Type 2 logic to capture historical changes in dimension tables, ensuring accurate tracking of data over time.

Finally, in the Gold layer, I created fact and dimension tables using the curated data from the Silver layer. This layer was optimized for analytics and reporting, enabling business users and analysts to derive meaningful insights from the data.
