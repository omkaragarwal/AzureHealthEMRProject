# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

client_id = '1ddb96c3-8572-408a-9c91-309239637dd4_75133d90-ca1d-495c-a3c9-341d1bf35d89'
client_secret = 'wUP4zRHyqt3xyuzeh2rb2Iw8quEZJPNPKte7A6u6ppI='
base_url = 'https://id.who.int/icd/'
scope = 'icdapi_access'
current_date = datetime.now().date()

auth_url = 'https://icdaccessmanagement.who.int/connect/token'
auth_response = requests.post(auth_url, data={
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type': 'client_credentials',
    'scope':scope
})


if auth_response.status_code == 200:
    access_token = auth_response.json()['access_token']
else:
    raise Exception(f'Failed to get access token : {auth_response.status_code} - {auth_response.text}')

headers = {
    'Authorization':f'Bearer {access_token}',
    'API-Version':'v2',
    'Accept': 'application/json', 
    'Accept-Language': 'en',
}

def fetch_icd_codes(url):
    response = requests.get(url, headers=headers)
    if(response.status_code == 200):
        return response.json()
    else:
        raise Exception(f'Failed to fetch data from {url} : {response.status_code} - {response.text}')

def extract_codes(url):
    data = fetch_icd_codes(url)
    codes =[]
    if 'child' in data:
        for child_url in data['child']:
            codes.extend(extract_codes(child_url))
    else:
        if 'code' in data and 'title' in data:
            # print(data['code'],data['title']['@value'])
            codes.append({
                'icd_code': data['code'],
                'icd_code_type': 'ICD-10',
                'code_description': data['title']['@value'],
                'inserted_date': current_date,
                'updated_date': current_date,
                'is_current_flag': True
            })
    return codes

root_url = 'https://id.who.int/icd/release/10/2019/A00-A09'
icd_codes = extract_codes(root_url)

schema = StructType([
    StructField("icd_code", StringType(), True),
    StructField("icd_code_type", StringType(), True),
    StructField("code_description", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
    StructField("is_current_flag", BooleanType(), True)
])

# Create a DataFrame using the defined schema
print(icd_codes)
df = spark.createDataFrame(icd_codes, schema=schema)
df.write.format("parquet").mode("append").save("/mnt/bronze/icd_codes/")