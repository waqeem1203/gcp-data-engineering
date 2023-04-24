# Submitting a Dataproc Job using Cloud Composer

You will be submitting a Dataproc Job to a Dataproc cluster using Cloud Composer. This uses the DataprocSubmitJobOperator airflow operator.

# Requirements

1. Cloud Composer
2. Cloud Dataproc
3. BigQuery
4. Cloud Storage

# Step 1: Creating your bucket

You can create a bucket using this [tutorial](https://cloud.google.com/storage/docs/creating-buckets) from Google.

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

Upload this script to the bucket you created

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

## Step 5: Setting up Cloud Composer

Go to Cloud Composer. Create an environment with `Composer 1`
![image](https://user-images.githubusercontent.com/50084105/234130038-5f333f08-7f9c-4e8e-a904-f24f3e4047ca.png)

Give it a name, select a location and image. Note: Not all zones will have an image available. Configure these setting and click `Create`.

![image](https://user-images.githubusercontent.com/50084105/234130240-2a13a930-6d32-4971-a27b-45a95e5ebd9d.png)

Composer takes a while to set up. Once it is complete it will look like this.

![image](https://user-images.githubusercontent.com/50084105/234132460-b698227a-f322-4e06-b557-603b65c95494.png)

## Step 6: Uploading the DAG File

Open the environment and click `Open DAGS Folder`

![image](https://user-images.githubusercontent.com/50084105/234130576-eb041a17-b9e3-4865-8cdc-939308755acf.png)

Upload this script to the bucket as a python file.

```

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago

PROJECT_ID = "<project name>"
REGION = "<composer region>"
BUCKET = "<bucket name>"
PYTHON_FILE_LOCATION = "gs://<bucket name>/dataproc_pyspark.py" # PySpark file from Step 4
CLUSTER_NAME = "<dataproc cluster created in Step 2>"
SPARK_BIGQUERY_JAR_FILE = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYTHON_FILE_LOCATION },
    "jarFileUris":[SPARK_BIGQUERY_JAR_FILE]
}

with models.DAG(
    "dataproc_job_operator",  # The id you will see in the DAG airflow page
    default_args=default_args,  # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    initiate_job =  DataprocSubmitJobOperator(
    task_id="pyspark_job", job=PYSPARK_JOB, region="<cluster region>", project_id=PROJECT_ID
    )

initiate_job
```

The DAG will run immediately and should run successfuly. The DAG we uploaded is `dataproc_job_operator`.

![image](https://user-images.githubusercontent.com/50084105/234131143-f62daf89-0312-497b-816b-7d712eeb810d.png)

This task started at 20:25 and ended at 20:27.

![image](https://user-images.githubusercontent.com/50084105/234131293-47b0de11-da1f-4faf-ae14-77252495b41a.png)

If we open Dataproc jobs we can see that the Dataproc job was triggered in less than a minute.

![image](https://user-images.githubusercontent.com/50084105/234131411-ae880a57-3a10-4349-880c-0ea37c7a22c8.png)

To see the desired result of this PySpark job please check this [project](https://github.com/waqeem1203/gcp-data-engineering/blob/main/Moving%20data%20from%20Cloud%20Storage%20to%20BiqQuery%20using%20Dataproc.md)

## Step 7: Cleaning up

Once you're done with the tutorial make sure you delete all the resources you create, so that you don't get unexpected costs.
