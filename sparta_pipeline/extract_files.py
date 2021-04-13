import boto3
import json
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.DEBUG)

s3 = boto3.client('s3')
bucket_name = 'data20-final-project'


def extract_csv(key):
    if re.findall(".csv$", key):
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body']
        return pd.read_csv(strbody)
    else:
        return "That is not a CSV file."

print(extract_csv('Talent/'))

def extract_json(key):
    if re.findall(".json$", key):
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body']
        data = strbody.read()
        return json.loads(data)
    else:
        return "That is not a JSON file."



