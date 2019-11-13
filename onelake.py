import json 
import config


# Get the s3 file count
def s3SelectFilecount(bucket, key):
    response = config.client.select_object_content(
    Bucket=bucket, 
    Key=key,
    Expression=config.s3selectcountquery,
    ExpressionType='SQL',
    InputSerialization={
        "Parquet": {
        },
    },
    OutputSerialization={
        'CSV': {}
    },
    )
    for event in response['Payload']:
        if 'Records' in event:
            return event['Records']['Payload'].decode('utf-8')

# get the s3 column name and count 
def s3SelectColumns(bucket, key):
    resp = config.client.select_object_content(
    Bucket=bucket, 
    Key=key,
    Expression=config.s3selectcolumnquery,
    ExpressionType='SQL',
    InputSerialization={
        "Parquet": {
         },
    },
    OutputSerialization={
        'JSON': {}
    },
    )
    for event in resp['Payload']:
        if 'Records' in event:
            return list(json.loads(event['Records']['Payload'].decode('utf-8')).keys())