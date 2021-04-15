from sparta_pipeline import extract_files
from sparta_pipeline import transformations
from sqlalchemy import *
import logging
import pandas as pd
import boto3
import re
from pprint import pprint

logging.basicConfig(level=logging.INFO)
s3 = boto3.client('s3')

bucket_name = 'data20-final-project'
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)
contents = bucket.objects.all()
students = [i.key for i in contents if re.findall(".json$", i.key)]
Keys = [file.key for file in contents]

with open("credentials.txt") as f1, open("..\\config.ini") as f2:
    line_file1 = f1.readlines()
    line_file2 = f2.readlines()
    converted = []
    for element in line_file1:
        converted.append(element.strip())
    for element in line_file2:
        converted.append(element.strip())

user = converted[0]
password = converted[1]
server = converted[2]
database = converted[3]
driver = converted[4]
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

connection = engine.connect()
meta = MetaData()


def load_courses_table():
    courses = []
    for key in Keys:
        if 'Academy' in key:
            temp = key[8:-15].split('_')
            if temp[0] not in courses:
                courses.append(temp[0])
    df = pd.DataFrame(courses, columns=['name'])
    logging.info(df)
    df.to_sql('courses', engine, index=False, if_exists="append")


def load_classes_table():
    classes = []
    for key in Keys:
        if 'Academy' in key:
            classes.append(key[8:-15])

    df = pd.DataFrame(classes, columns=['name_number'])
    logging.info(df)
    df.to_sql('classes', engine, index=False, if_exists="append")


def load_student_information():
    student_id = []
    si = []
    for i in students[1:21]:
        si.append(transformations.convert_si(extract_files.extract_json(i)))
        student_id.append(re.split("[/.]", i)[1])
    df = pd.concat(si).reset_index()
    df2 = pd.DataFrame(id, columns=["student_id"])
    output = pd.concat([df2, df], axis=1)
    del output["index"]
    # logging.info(df)
    # logging.info(df2)
    logging.info(output)


def load_behaviours():
    pass


def load_weeks():
    pass


def load_tech_types_table():
    techs = ['C#', 'C++', 'Java', 'JavaScript', 'PHP', 'Python', 'R', 'Ruby', 'SPSS']
    df = pd.DataFrame(techs, columns=['name'])
    logging.info(df)
    df.to_sql('tech_types', engine, index=False, if_exists="append")


def load_self_score():
    pass


def load_strengths():
    strengths = ['Charisma', 'Patient', 'Curious', 'Problem Solving', 'Courteous', 'Independent', 'Passionate',
                 'Versatile', 'Rational', 'Collaboration', 'Ambitious', 'Reliable', 'Altruism', 'Empathy', 'Listening',
                 'Organisation', 'Consistent', 'Efficient', 'Determined', 'Composure', 'Competitive', 'Perfectionism',
                 'Innovative', 'Creative', 'Critical Thinking']
    df = pd.DataFrame(strengths, columns=['name'])
    logging.info(df)
    df.to_sql('strength_types', engine, index=False, if_exists="append")


def load_student_strengths():
    pass


def load_weaknesses():
    weakness_types = [
        'Distracted', 'Impulsive', 'Introverted', 'Overbearing', 'Chatty', 'Indifferent', 'Anxious', 'Perfectionist',
        'Sensitive', 'Controlling', 'Immature', 'Impatient', 'Conventional', 'Undisciplined', 'Passive', 'Intolerant',
        'Chaotic', 'Selfish', 'Slow', 'Competitive', 'Critical', 'Indecisive', 'Procrastination', 'Stubborn']
    df_weakness_types = pd.DataFrame(weakness_types, columns=['name'])
    logging.info(df_weakness_types)
    df_weakness_types.to_sql('weakness_types', engine, index=False, if_exists="append")


def load_student_weaknesses():
    pass


def load_scores():
    pass


def load_personal_information():
    data = extract_files.extract_csv('Talent/July2019Applicants.csv')
    transformed_data = transformations.convert_pi(data)
    del transformed_data["id"]
   

def main():
    load_student_information()


main()
