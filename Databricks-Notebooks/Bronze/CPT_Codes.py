# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# Read the CSV file
cptcodes_df = spark.read.csv("/mnt/landing/cpt_codes/*.csv", header=True)

for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)
display(cptcodes_df)

cptcodes_df.write.format("parquet").mode("overwrite").save("/mnt/bronze/cpt_codes")
