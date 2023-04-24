# This tutorial will show you how to write the data in a file to BigQuery as soon as it is uploaded on Cloud Storage.

## Requirements

1. Cloud Storage Bucket
2. Cloud Functions
3. BigQuery

## Step 1: Setting up your Cloud Storage Bucket

You can create a bucket using this [tutorial](https://cloud.google.com/storage/docs/creating-buckets) from Google.

## Step 2: Setting up BigQuery

Open BigQuery. Run the query below to create a dataset.

```

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
