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
import json

logging.basicConfig(level=logging.INFO)
s3 = boto3.client('s3')

bucket_name = 'data20-final-project'
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket = s3_resource.Bucket(bucket_name)
contents = bucket.objects.all()
students = [i.key for i in contents if re.findall(".json$", i.key)]
courses = [i.key for i in contents if re.findall(".csv$", i.key) and re.findall("^Academy", i.key)]
applicants = [i.key for i in contents if re.findall(".csv$", i.key) and re.findall("^Talent", i.key)]
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

engine = create_engine(f"mssql+pyodbc://{user}:{password}@{userinfo['server']}/"
                       f"{userinfo['database']}?driver={userinfo['driver']}")
connection = engine.connect()
meta = MetaData()


def load_courses_table():
    df = transformations.course_trainers()[0]
    # df.to_sql('courses', engine, index=False, if_exists="append")
    print(df)


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
    temp_info = transformations.get_unique_column_json("tech_self_score", students)
    print(temp_info)

    # for tech in extract_files.extract_json(students)["tech_self_score"]:
    #     if tech not in techs:
    #         techs.append(tech)
    #     else:
    #         pass
    # print(techs)
    # df = pd.DataFrame(techs, columns=['name'])
    # logging.info(df)
    # df.to_sql('tech_types', engine, index=False, if_exists="append")

# load_tech_types_table()


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
    student_scores = []
    for file in s_day[1:21]:
        # Extract each file, including headers
        for student in extract_files.extract_txt(file):
            # Remove headers by checking for string not found in headers
            if "Psychometrics" in student:
                # Split string elements into list, must have a space either side of "-" for double-barrel name
                name_scores = re.split(" - |,|:", student)
                # Iterate across list elements, strip whitespace
                for i in range(len(name_scores)):
                    name_scores[i] = name_scores[i].strip()
                # Remove exam titles
                name_scores.remove("Psychometrics")
                name_scores.remove("Presentation")
                # Split Psych and Presentation Scores, append maximum scores for each
                for j in [1, 2]:
                    name_scores.append(name_scores[j].split("/")[1])
                    name_scores[j] = name_scores[j].split("/")[0]
                # Turn numbers into integer data types
                for i in range(1, 5):
                    name_scores[i] = int(name_scores[i])
                # Swap columns round so Psych and Pres columns are together
                name_scores[2], name_scores[3] = name_scores[3], name_scores[2]
                student_scores.append(name_scores)

    df_scores = pd.DataFrame(student_scores, columns=['full_name', 'psych_score', 'psych_max_score',
                                                      'presentation_score', 'presentation_max_score'])
    logging.info(df_scores)


def load_personal_information():
    data = extract_files.extract_csv('Talent/July2019Applicants.csv')
    transformed_data = transformations.convert_pi(data)


def load_staff_information():
    final_df = transformations.convert_staff_information()
    logging.info(final_df)
    # final_df.to_sql('staff_information', engine, index=False, if_exists="append")


def main():
    pass


main()
