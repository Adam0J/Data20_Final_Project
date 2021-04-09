import boto3
import json
import pandas as pd


s3 = boto3.client('s3')
bucket_name = 'data20-final-project'
bucket_contents = s3.list_objects_v2(Bucket=bucket_name)
keys = [file['Key'] for file in bucket_contents['Contents']]


def extract_csv():
    csv_keys = []
    csv_dfs = []
    for key in keys:
        if 'Academy' in key:
            csv_keys.append(key)

    for key in csv_keys:
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body']
        initial_df = pd.read_csv(strbody)
        csv_dfs.append(initial_df)
    
    return csv_dfs


def extract_json():
    json_keys = []
    json_files = []
    for key in keys:
        if 'Talent' in key:
            json_keys.append(key)

    for key in json_keys:
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body']
        data = strbody.read()
        obj = json.loads(data)
        json_files.append(obj)

    return json_files


