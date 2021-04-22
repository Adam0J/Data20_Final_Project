import boto3
import json
import pandas as pd
import re

s3 = boto3.client('s3')
bucket_name = 'data20-final-project'

s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)
contents = bucket.objects.all()


def extract_csv(key):
    if re.findall(".csv$", key):
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body']
        return pd.read_csv(strbody)
    else:
        raise Exception("That is not a CSV file.")


def extract_json(key):
    if re.findall(".json$", key):
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body'].read()
        return json.loads(strbody)
    else:
        raise Exception("That is not a JSON file.")


def extract_txt(key):
    if re.findall(".txt$", key):
        s3_object = s3.get_object(
            Bucket=bucket_name,
            Key=key)
        strbody = s3_object['Body'].read()
        return strbody.decode('utf-8').splitlines()
    else:
        raise Exception("That is not a TEXT file.")

