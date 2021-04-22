import boto3
import json
import pandas as pd
from pprint import pprint
import re


# bucket_name = 'data20-final-project'
# s3_resource = boto3.resource('s3')
# s3_client = boto3.client('s3')
# bucket = s3_resource.Bucket(bucket_name)
# contents = bucket.objects.all() # iterable
# Keys = [file.key for file in contents]
# classes = []
s3 = boto3.client('s3')
bucket_name = 'data20-final-project'

s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)
contents = bucket.objects.all()

students = []
courses = []
applicants = []
s_day = []


def sort_keys():
    for i in contents:
        if re.findall(".json$", i.key):
            students.append(i.key)
        elif re.findall("^Academy", i.key):
            courses.append(i.key)
        elif re.findall(".csv$", i.key):
            applicants.append(i.key)
        elif re.findall(".txt$", i.key):
            s_day.append(i.key)
sort_keys()

for key in s_day:
    s3_object = s3.get_object(
        Bucket=bucket_name,
        Key=key)
    strbody = s3_object['Body'].read()
    a = strbody.decode('utf-8').splitlines()
print(a)
# t_keys = []
# for key in Keys:
#     if 'Talent' in key:
#         t_keys.append(key)

# cc = []
# for key in t_keys:
#     s3_object = s3_client.get_object(
#     Bucket = bucket_name,
#     Key = key)
#
#     pd.set_option("display.max_rows", None, "display.max_columns", None)
#
#     strbody = s3_object['Body']
#     data=strbody.read()
#     obj = json.loads(data)
#     try:
#         if obj['course_interest'] not in cc:
#             cc.append(obj['course_interest'])
#         else:
#             print(cc)
#
#     except:
#         print(cc)
#
# print(cc)

# s3_object = s3.get_object(
#     Bucket = bucket_name,
#     Key = 'Academy/Engineering_23_2019-08-12.csv')
#
# pd.set_option("display.max_rows", None, "display.max_columns", None)
# strbody = s3_object['Body']
# data=strbody.read()
# obj = json.loads(data)
# print(obj['tech_self_score'].keys())