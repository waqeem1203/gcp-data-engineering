# Move Data from PubSub to BigQuery using Cloud DataFlow 

Data will be streamed from the CoinCap API to PubSub. This data will be written to a BigQuery table using Cloud DataFlow.

## Requirements

1. PubSub
2. BigQuery
3. Cloud Storage

## Step 1: Create a bucket

You can create a bucket using this [tutorial](https://cloud.google.com/storage/docs/creating-buckets) from Google.

## Step 2: Setting up BigQuery

Open BigQuery. Run the query below to create a schema.

```
CREATE SCHEMA `project_id.schema_name`
```

Once the schema is created, you can run the query below to create the destination table for your data.

```
create table `project_id.schema_name.table_name`
(
  id STRING,
  rank INT64,
  symbol STRING,
  name STRING,
  supply BIGNUMERIC,
  maxSupply BIGNUMERIC,
  marketCapUsd BIGNUMERIC,
  volumeUsd24Hr BIGNUMERIC,
  priceUsd BIGNUMERIC,
  changePercent24Hr BIGNUMERIC,
  vwap24Hr BIGNUMERIC,
  explorer STRING,
  timestamp TIMESTAMP
)
```

## Step 3: Setting up PubSub

1. Go to PubSub > Topics > Create Topic.
2. Give a Topic ID and check `Use a Schema`. 
3. Under `Select a Pub/Sub schema` select `Create New Schema`
4. Select `Avro` as the schema type and paste the below snippet in the schema definition.

```
{
    "type" : "record",
    "name" : "Avro",
    "fields" : [
      {
        "name" : "id",
        "type" : "string"
      },
      {
        "name" : "rank",
        "type" : "int"
      },
      {
        "name" : "symbol",
        "type" : "string"
      },
      {
        "name" : "name",
        "type" : "string"
      },
      {
        "name" : "supply",
        "type" : "double"
      },
      {
        "name" : "maxSupply",
        "type" : "double"
      },
      {
        "name" : "marketCapUsd",
        "type" : "double"
      },
      {
        "name" : "volumeUsd24Hr",
        "type" : "double"
      },
      {
        "name" : "priceUsd",
        "type" : "double"
      },
      {
        "name" : "changePercent24Hr",
        "type" : "double"
      },
      {
        "name" : "vwap24Hr",
        "type" : "double"
      },
      {
        "name" : "explorer",
        "type" : "string"
      },
      {
        "name" : "timestamp",
        "type" : "int",
        "logicalType": "time-millis"
      }
    ]
   }
```

5. Validate the schema definition. You can use this example to test the schema.

```
{
    "id":"audius",
    "rank":100,
    "symbol":"AUDIO",
    "name":"Audius",
    "supply":997315329.0000000000000000,
    "maxSupply":0,
    "marketCapUsd":313958600.7016208291051982,
    "volumeUsd24Hr":6518393.7831468042287091,
    "priceUsd":0.3148037451870158,
    "changePercent24Hr":-5.6418599800119077,
    "vwap24Hr":0.3165565608345000,
    "explorer":"https://etherscan.io/token/0x18aaa7115705e8be94bffebde57af9bfc265b998",
    "timestamp":1682082324
}
```

6. Click Create.
7. Go back to the Create Topic Page. Select the schema you just created and leave the other settings as is.
8. Create a topic.

## Step 4: Setting up PubSub Subscription

1. Select the topic you just created and click `Create Subscription`. 
2. Set the delivery type as `Pull` and leave the other settings as is. 
3. Click `Create`. 
4. Remember to give a service account access to this subscription so that you can read messages to PubSub later. You can use this [tutorial](https://github.com/waqeem1203/gcp-data-engineering/blob/main/Create%20a%20Service%20Account.md) to create one.

## Step 5: Set up Dataflow

1. Go to DataFlow > Jobs. 
2. Click `Create Job from Template`
3. Select the `Pub/Sub Subscription to BigQuery` dataflow template. 
4. Select the PubSub subscription you created earlier.
5. Select a temporary directory for DataFlow to process data.
6. Click `Run Job`

You should see a streaming job get created.

![image](https://user-images.githubusercontent.com/50084105/234135256-c267ac26-319d-4747-8f3d-1afd73f7314c.png)

## Step 6: Testing the pipeline.

Run the script below to send data to PubSub. You will need a service account and download the [keys](https://github.com/waqeem1203/gcp-data-engineering/blob/main/Create%20a%20Service%20Account.md) to run this script.

```
from google.cloud import pubsub_v1
import requests
import datetime as DT
import json
import os

def get_asset_data():
    url = "https://api.coincap.io/v2/assets"
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    asset_data = json.loads((response.text))
    assets = []
    ts = asset_data["timestamp"]
    for asset in asset_data["data"]:    
        #assets 
        asset["timestamp"] = int(ts/1000)
        asset["rank"] = int(asset["rank"])
        asset["supply"] = float(asset["supply"])
        if asset["maxSupply"] == None:
            asset["maxSupply"] = asset["supply"]
        else:
            asset["maxSupply"] = float(asset["maxSupply"])
        asset["marketCapUsd"] = float(asset["marketCapUsd"])
        asset["volumeUsd24Hr"] = float(asset["volumeUsd24Hr"])
        asset["priceUsd"] = float(asset["priceUsd"])
        asset["changePercent24Hr"] = float(asset["changePercent24Hr"])
        if asset["vwap24Hr"] == None:
            asset["vwap24Hr"] = float(0)
        else:
            asset["vwap24Hr"] = float(asset["vwap24Hr"])
        if asset["explorer"] == None:
            asset["explorer"] = ""
        assets.append(json.dumps(asset))
    return assets

asset_data = get_asset_data()

# TODO(developer)
project_id = "<project id>"
topic_id = "<topic id>"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'<credentials json file>.json'

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for i in asset_data:
   # When you publish a message, the client returns a future.
    data = i.encode("utf-8")
    #print(data)
    future = publisher.publish(topic_path, data)
    # print(future.result())

print(f"Published messages to {topic_path} at {str(DT.datetime.utcnow())}.")
```

This script was run at 23:24 UTC.

![image](https://user-images.githubusercontent.com/50084105/234136633-fafa4ea3-b9b4-4fcc-85f6-d5fa9f2276b5.png)

You can run this query to see the latest entry in the BigQuery table, this matches the upload time of the script.

![image](https://user-images.githubusercontent.com/50084105/234137227-9752cd22-5dc1-4b0e-91ec-1311f93604fd.png)

## Step 7: Cleaning up

Once you're done with the tutorial make sure you delete all the resources you create, so that you don't get unexpected costs.





