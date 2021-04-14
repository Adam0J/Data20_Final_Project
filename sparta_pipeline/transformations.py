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


data = extract_files.extract_json("Talent/10384.json")
dataCsv = extract_files.extract_csv("Academy/Data_28_2019-02-18.csv")


logging.basicConfig(level=logging.INFO)

pd.set_option("display.max_rows", None, "display.max_columns", None)

si_columns = ["name", "date", "self_development", "geo_flex", "financial_support_self", "result"]
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
    :param info: this will be a dictionary
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
    temp = [re.sub('[^+0-9]', '', i) for i in info.get("phone_number").values.tolist() if i is not None]
    new = pd.DataFrame(temp, columns=["phone_number"])
    info.update(new)
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



