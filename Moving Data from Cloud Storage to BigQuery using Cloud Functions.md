# Using Cloud Functions to write data from Cloud Storage to BigQuery

We will be extracting data from the CoinCap API and uploading the data to Google Cloud Storage. Cloud Functions will write the contents on this file to a table in BigQuery as soon as it is uploaded.

## Requirements

1. Cloud Storage Bucket
2. Cloud Functions
3. BigQuery

## Step 1: Setting up your Cloud Storage Bucket

You can create a bucket using this [tutorial](https://cloud.google.com/storage/docs/creating-buckets) from Google.

## Step 2: Setting up BigQuery

Open BigQuery. Run the query below to create a schema.

```
CREATE SCHEMA `project_id.schema_name`
```

Once the schema is created, you can run the query below to create the destination table for your data. This will be needed in `Step 3`.

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


## Step 3: Setting up Cloud Functions

Go to Cloud Functions and click on `Create Function`. In the first page configure the settings below.

![image](https://user-images.githubusercontent.com/50084105/234111395-4d84e829-64e8-438f-9ede-f9b21e333568.png)

Click `Add Eventarc Trigger` and configure these settings, you will need to enter the bucket name that you created in Step 1 in the `Bucket` field.

![image](https://user-images.githubusercontent.com/50084105/234111902-1da8f53a-5990-4e4d-b1f4-227bf79d2590.png)

You will need to increase the Memory Allocated to `512 MiB`. Click `Next` once done.

![image](https://user-images.githubusercontent.com/50084105/234112389-f642fb50-0915-449c-afae-015684d2bfb4.png)

On page 2 select these settings.

![image](https://user-images.githubusercontent.com/50084105/234112912-f5253fe8-8007-4c75-8722-f4ba6e5e870f.png)

In the `main.py` file replace the code with the snippet below:

```
import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

def gcs_to_bq(event, context):
    lst = []
    file_name = event['name']
    table_name = file_name.split('/')[0]
    
    dct = {
        'Event_ID':context.event_id,
        'Event_type':context.event_type,
        'Bucket_name':event['bucket'],
        'File_name':event['name'],
        'Created':event['timeCreated'],
        'Updated':event['updated']
    }
    lst.append(dct)
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq('coincapdb.file_upload',
                    project_id = '<project id>',
                    if_exists = 'append',
                    location = 'us')
    df_data = pd.read_parquet(f"gs://{event['bucket']}/{file_name}")
    df_data.to_gbq('<BigQuery table from step 2>', 
                        project_id='<project id>', 
                        if_exists='append',
                        location='us')
```

In `requirements.txt` add these libraries and click `Deploy`.

```
functions-framework==3.*
gcsfs
fsspec
pandas
pandas-gbq
```
## Step 4: Testing the Function

You will get a screen like this when the function is deployed.

![image](https://user-images.githubusercontent.com/50084105/234116518-356a0163-5268-4efe-bbe2-3ae2091f69ca.png)

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
            rate["timestamp"] = runtime_ts
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
bucket = client.get_bucket('<your bucket>')
file_name = "data_"+  DT.datetime.now().strftime("%Y-%b-%d-%H-%M")  
df.to_parquet(f'<file path C:/dir1/dir2>/{file_name}.parquet')
bucket.blob(f'asset-exchange-rate-history/{file_name}.parquet').upload_from_filename(f'<file path C:/dir1/dir2>/{file_name}.parquet')
print(f'\n{file_name} uploaded to cloud storage')
```

This file was uploaded at 4:02 PM local time.

![image](https://user-images.githubusercontent.com/50084105/234116949-1ba15c53-6319-4812-86ce-79012069b6ad.png)

We can see in cloud storage file that the file was uploaded at 4:03 PM

![image](https://user-images.githubusercontent.com/50084105/234117121-595d52ee-735d-472a-a2ab-c9e8151697a7.png)

In the cloud function logs we can see that the function was triggered and had succeeeded.

![image](https://user-images.githubusercontent.com/50084105/234117737-091cf658-c7b0-4537-aef7-9da5b25d1748.png)

To verify this we can check `Last modified` on the details page of the BigQuery table.
Note: 
1. The cloud function creates a table for the directory the file is stored in. In this example the name of the directory is `asset-exchange-rate-history`. 
2. If no table with the name had existed in the schema, cloud functions would have created the table instead. This is why the timestamps in the screenshot below are the same. 
3. This function only works for a single level directory. i.e `bucket/directory1/file` will work but `bucket/directory1/directory2/file` will fail.

![image](https://user-images.githubusercontent.com/50084105/234118429-246c3d58-6c85-49c2-aa89-a4ed592e2538.png)



