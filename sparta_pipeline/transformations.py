from sqlalchemy import *
import pandas as pd
from sparta_pipeline import extract_files
import boto3
from pprint import pprint
import re
import logging
import time
import itertools
import re
from datetime import datetime

data = extract_files.extract_json("Talent/10384.json")
dataCsv = extract_files.extract_csv("Academy/Data_28_2019-02-18.csv")
data_app = 'Talent/Feb2019Applicants.csv'
logging.basicConfig(level=logging.INFO)

pd.set_option("display.max_rows", None, "display.max_columns", None)

si_columns = ["name", "date", "self_development", "geo_flex", "financial_support_self", "result", "course_interest"]
weeks_columns = ["student_id", "week_id", "behaviour_id", "score"]
courses_column = "name"
courses = []


def convert_si(info):
    """
    :param info: dictionary of a student's info
    :return: dataframe of a single student to load, excludes primary/foreign keys
    """
    to_load = {}
    for i in si_columns:
        if i in info:
            # logging.info(i)
            # logging.info(info.get(i))
            if info.get(i) in ["Yes", "Pass"]:
                to_load.update({i: 1})
            elif info.get(i) in ["No", "Fail"]:
                to_load.update({i: 0})
            else:
                to_load.update({i: info[i]})
    return pd.DataFrame(to_load, index=[0])


def convert_scores(info):
    """
    :param info: this will be a s3 key
    :return: will be dataframe
    """
    new_list = [re.split(', | -  |: |/', i) for i in info]
    student_scores = []
    for student in new_list[3:]:
        psyc_score = int(student[2])
        psyc_max = int(student[3])
        pres_score = int(student[5])
        pres_max = int(student[6])
        student_scores.append([psyc_score, psyc_max, pres_score, pres_max])
    return pd.DataFrame(student_scores)


def convert_pi(info):
    """
    :param info: this will be a dataframe
    :return: will be dataframe
    """
    info["phone_number"] = info["phone_number"].fillna("0")
    info["invited_date"] = info["invited_date"].fillna("Not")
    info["month"] = info["month"].fillna("Invited")

    temp = [re.sub('[^+0-9]', '', i) for i in info.get("phone_number").values.tolist() if i is not None]
    temp_day = [str(int(i)) if isinstance(i, float) else i for i in info.get("invited_date").values.tolist()]
    temp_month = [i for i in info.get("month").values.tolist()]
    temp_date = [datetime.strptime(x+" "+y, "%d %B %Y").date() if x+y != "NotInvited" else x+" "+y
                 for x, y in zip(temp_day, temp_month)]

    new = pd.DataFrame({"phone_number": temp, "invited_date": temp_date})
    info.update(new)
    del info["month"]
    return info


def convert_weeks(info):
    """
    :param info: this will be a dataframe
    :return: should be dataframe
    """
    # format the dataframe from wide to long
    new_df = info.melt(id_vars=["name", "trainer"], var_name="behaviours", value_name="score")

    # calculating number of weeks in the file
    weeks = int(len(new_df) / 6)
    number_of_weeks = int(weeks / len(info))

    # iterating week_id across all students
    lst = range(1, number_of_weeks + 1)
    wks_col = list(itertools.chain.from_iterable(itertools.repeat(x, int((len(new_df))/number_of_weeks)) for x in lst))

    # adding week_id column
    new_df["week_id"] = wks_col

    # Removing .._W<number> from behaviours
    behaviours = ["Analytic", "Independent", "Determined", "Professional", "Studious", "Imaginative"]
    new_b = behaviours * int(len(new_df) / 6)
    new_df2 = pd.DataFrame(new_b, columns=["behaviours"])
    new_df.update(new_df2)

    # return dataframe and drop students who dropped out
    return new_df.sort_values(by=["name", "week_id"]).dropna()


def convert_courses(info):
    """
    probably won't need this
    :param info:
    :return:
    """
    to_load_courses = {}
    for entry in info:
        if entry == "course_interest":
            # only add course if it's not already been added
            if str(info[entry]) not in courses:
                to_load_courses.update({courses_column: info[entry]})
                courses.append(info[entry])
            else:
                continue
    return pd.DataFrame(to_load_courses, index=[0])


def sparta_location(key):
    file_contents = extract_files.extract_txt(key)
    names = [re.split(" - ", i)[0] for i in file_contents[3:]]
    name_df = pd.DataFrame(names, columns=["full_name"])
    name_df["location"] = file_contents[1]
    return name_df

