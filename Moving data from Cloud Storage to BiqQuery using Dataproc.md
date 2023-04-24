# Moving data from Cloud Storage to BigQuery using Dataproc

This tutorial writes data uploaded to a Cloud Storage bucket to BigQuery, moves this file to a cold bucket and deletes it from the main bucket.

## Requirements

1. Google Cloud Storage
2. Cloud Dataproc
3. BigQuery

## Step 1: Creating your buckets

You can create a bucket using this [tutorial](https://cloud.google.com/storage/docs/creating-buckets) from Google. You will need to create two buckets.

## Step 2: Create a Dataproc Cluster

Go to `Dataproc` > `Cluster` > `Create Cluster`. Select `Cluster on Compute Engine`.

Leave the settings as it is and select `Single Node (1 master, 0 workers)` as the cluster type. Click `Create`.

![image](https://user-images.githubusercontent.com/50084105/234123169-07b991c0-fd13-4d8a-b929-6cfe31e2aee3.png)

The cluster will start running after a while.

![image](https://user-images.githubusercontent.com/50084105/234123611-aacc02ff-68a7-49d5-81db-2d4e6b6b82b3.png)

## Step 3: Setting up BigQuery

Open BigQuery. Run the query below to create a schema.

```
CREATE SCHEMA `project_id.schema_name`
```

Once the schema is created, you can run the query below to create the destination table for your data. This will be needed in `Step 4`.

```
create table `project_id.schema_name.table_name`
(
  exchangeId STRING,
  baseId STRING,
  quoteId STRING,
  baseSymbol STRING,
  quoteSymbol STRING,
  volumeUsd24Hr FLOAT64,
  priceUsd FLOAT64,
  volumePercent FLOAT64,
  timestamp TIMESTAMP
)

```

## Step 4: Upload the PySpark Script

Upload this script to the primary bucket

```
import pyspark
import sys
from google.cloud import storage
from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql.types import *


def list_blobs(bucket_name):
    bucket_name = bucket_name
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    files = []
    for blob in blobs:
        if "asset-exchange-rate-history/" in blob.name and ".parquet" in blob.name:
            files.append(f'gs://{bucket_name}/{blob.name}')
    return files

list = list_blobs('<primary bucket>')
print(list)

main_storage_client = storage.Client()
bucket_name = "<primary bucket>"
cold_bucket = "<archive bucket>"
sc = SparkSession.builder.appName('NotOverPySpark') \
                    .config('spark.jars.packages', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar') \
                    .getOrCreate()
for file in list:
    df = sc.read.format("parquet").load(file)
    bq_df = df.withColumn("volumeUsd24Hr",df.volumeUsd24Hr.cast(FloatType())) \
    .withColumn("priceUsd",df.priceUsd.cast(FloatType())) \
    .withColumn("volumePercent",df.volumePercent.cast(FloatType())) \
    .withColumn('timestamp', (df.timestamp.cast(TimestampType())))
    # bq_df.printSchema()
    bq_df.write.format('bigquery').mode('append').option("writeMethod", "direct").save('<destination table from step 3>')
    print(f'{file} written to <destination table from step 3>')
    main_bucket = main_storage_client.bucket(bucket_name)
    blob_name = file.replace(f'gs://{bucket_name}/',"")
    cold_blob=(f'gs://{cold_bucket}/{blob_name}').replace(".parquet","")
    df.write.format("parquet").save(cold_blob)
    print(f'{file} written to {cold_blob}')
    blob = main_bucket.blob(blob_name)
    blob.delete()
    print(f'{file} deleted')
```

## Step 5: Submit a job to the Dataproc Cluster

Run the code below to upload a parquet file to your cloud storage bucket. You will need a service account and download the [keys](https://github.com/waqeem1203/gcp-data-engineering/blob/main/Create%20a%20Service%20Account.md) to run this script.

```
from google.cloud import storage
import os
import pandas as pd
import requests
import json
import datetime as DT
import os

def get_coin_prices(coin_list):
    payload = ""
    headers = {}
    coin_rates = []
    for coin in coin_list:
        url= "https://api.coincap.io/v2/assets/"+coin+"/markets"
        print(f"\rCoin Data Extracted: {coin_list.index(coin)+ 1}/{len(coin_list)}", end='', flush=True)
        response = requests.request("GET", url, headers=headers, data=payload)
        rates_response = json.loads(response.text)
        ts = rates_response["timestamp"]
        runtime_ts = DT.datetime.utcfromtimestamp(ts/1000).isoformat()
        for rate in rates_response["data"]:
            rate["timestamp"] = round(ts/1000,0)
            coin_rates.append(rate)  
    return coin_rates   

def get_asset_data():
    url = "https://api.coincap.io/v2/assets"
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    asset_data = json.loads(response.text)
    coins = []
    for asset in asset_data["data"]:
        coins.append(asset["id"])
    return coins

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'<Location of your Service Account Credentials json file>'

coin_list = get_asset_data()
coin_rates_list = get_coin_prices(coin_list)
json_object = json.dumps(coin_rates_list, indent=4)

df = pd.DataFrame(data = coin_rates_list,columns=['exchangeId','baseId','quoteId','baseSymbol','quoteSymbol','volumeUsd24Hr','priceUsd','volumePercent','timestamp'])
client = storage.Client()
bucket = client.get_bucket('<primary bucket>')
file_name = "data_"+  DT.datetime.now().strftime("%Y-%b-%d-%H-%M")  
df.to_parquet(f'< file path C:/Users/dir >/{file_name}.parquet')
bucket.blob(f'asset-exchange-rate-history/{file_name}.parquet').upload_from_filename(f'< file path C:/Users/dir >/{file_name}.parquet')
print(f'\n{file_name} uploaded to cloud storage')
```

This file should be visible in the `asset-exchange-rate-history` directory that was created in your bucket.

![image](https://user-images.githubusercontent.com/50084105/234126688-2fb98515-0210-4338-b02f-5fd2b0e4bdaa.png)


Go to `Dataproc` > `Jobs` > `Submit Job`.

1. Select the cluster you created in `Step 2`.
2. Select the job type as PySpark.
3. Enter the path to the PySpark file.
4. Add `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar` to Jar Files.

![image](https://user-images.githubusercontent.com/50084105/234124111-7496ef1d-a4da-4d3e-a553-6e91befec534.png)

In the logs we can see that the files are being written to BigQuery and then copied to the secondary bucket.

![image](https://user-images.githubusercontent.com/50084105/234128300-a900b880-40d3-4394-9c43-0b3bbc3b834a.png)

Wait till the job completes successfully.

![image](https://user-images.githubusercontent.com/50084105/234127163-7234d599-502f-40e0-99fe-63676fcf70da.png)

The files are now in the secondary bucket.

![image](https://user-images.githubusercontent.com/50084105/234127470-1fee3d8a-29e4-459d-b16b-c6c1d9d1e77a.png)






