from sparta_pipeline import extract_files
from sparta_pipeline import transformations
from sqlalchemy import *
import logging
import pandas as pd
import boto3
import re
from configparser import ConfigParser
import time
from pprint import pprint

logging.basicConfig(level=logging.INFO)
s3 = boto3.client('s3')

bucket_name = 'data20-final-project'
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)
contents = bucket.objects.all()
students = [i.key for i in contents if re.findall(".json$", i.key)]
courses = [i.key for i in contents if re.findall(".csv$", i.key) and re.findall("^Academy", i.key)]
s_day = [i.key for i in contents if re.findall(".txt$", i.key)]

# Read config.ini file
config_object = ConfigParser()
config_object.read("../config.ini")

# Get the userinfo
userinfo = config_object["USERINFO"]

with open("..\\credentials.txt") as f1:
    line_file1 = f1.readlines()
    converted = []
    for element in line_file1:
        converted.append(element.strip())


user = converted[0]
password = converted[1]

# engine = create_engine(f"mssql+pyodbc://{user}:{password}@{userinfo['server']}/"
#                        f"{userinfo['database']}?driver={userinfo['driver']}")
# connection = engine.connect()
# meta = MetaData()


def load_courses_table():
    list_courses = []
    for key in courses:
        temp = key[8:-15].split('_')
        course_code = temp[0] + ' ' + temp[1]
        list_courses.append(course_code)
    df = pd.DataFrame(list_courses, columns=['name_number'])
    logging.info(df)
    df.to_sql('classes', engine, index=False, if_exists="append")


def all_locations():
    output = []
    for i in s_day:
        output.append(transformations.sparta_location(i))
    return pd.concat(output)


def load_student_information():
    student_id = []
    si = []
    location_df = all_locations()
    for i in students:
        si.append(transformations.convert_si(extract_files.extract_json(i)))
        student_id.append(re.split("[/.]", i)[1])
    df = pd.concat(si).reset_index()
    df2 = pd.DataFrame(student_id, columns=["student_id"])
    output = pd.concat([df2, df], axis=1)
    output.rename(columns={"result": "passed", "date": "invited_date"}, inplace=True)
    id_name = pd.concat([output["student_id"], output["name"], output["invited_date"]], axis=1)
    output_merged = pd.merge(output, location_df, left_on=output["name"].str.lower(),
                             right_on=location_df["full_name"].str.lower(), how="inner")

    del output_merged["index"]
    del output_merged["key_0"]
    del output_merged["name"]
    del output_merged["full_name"]
    return output_merged, id_name


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
    start = time.time()
    # load_student_information()
    logging.info(load_student_information()[0])
    # logging.info(load_student_information()[1])
    end = time.time()
    print(end - start)


main()
