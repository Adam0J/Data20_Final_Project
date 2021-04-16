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
        # print(key)
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
    # print(si)
    # print(student_id)
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
    techs = []
    for tech in extract_files.extract_json(students)["tech_self_score"]:
        if tech not in techs:
            techs.append(tech)
        else:
            pass
    print(techs)
    df = pd.DataFrame(techs, columns=['name'])
    logging.info(df)
    df.to_sql('tech_types', engine, index=False, if_exists="append")
load_tech_types_table()


def load_self_score():
    pass


def load_strengths():
    list_strengths = []
    for student in students[1:21]:
        # check strengths for each student in JSON file
        for strength in extract_files.extract_json(student)["strengths"]:
            # append strength if it's not already in the list
            if strength not in list_strengths:
                list_strengths.append(strength)
            else:
                pass
    # convert to DataFrame to be loaded into table
    df_strengths = pd.DataFrame(list_strengths, columns=['name'])
    logging.info(df_strengths)
    # df.to_sql('strength_types', engine, index=False, if_exists="append")


def load_student_strengths():
    pass


def load_weaknesses():
    list_weaknesses = []
    for student in students[1:21]:
        # check weaknesses for each student in JSON file
        for weakness in extract_files.extract_json(student)["weaknesses"]:
            # append weakness if it's not already in the list
            if weakness not in list_weaknesses:
                list_weaknesses.append(weakness)
            else:
                pass
    # convert to DataFrame to be loaded into table
    df_weaknesses = pd.DataFrame(list_weaknesses, columns=['name'])
    logging.info(df_weaknesses)
    # df_weaknesses.to_sql('weakness_types', engine, index=False, if_exists="append")


def load_student_weaknesses():
    pass


def load_scores():
    pass


def load_personal_information():
    data = extract_files.extract_csv('Talent/July2019Applicants.csv')
    transformed_data = transformations.convert_pi(data)
    del transformed_data["id"]
   

def main():
    # load_courses_table()
    # load_weaknesses()
    load_strengths()
    # start = time.time()
    # # load_student_information()
    # logging.info(load_student_information()[0])
    # # logging.info(load_student_information()[1])
    # end = time.time()
    # print(end - start)


main()
