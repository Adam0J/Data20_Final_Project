from sqlalchemy import *
import pandas as pd
import extract_files
import boto3
from pprint import pprint
import logging
import time

logging.basicConfig(level=logging.INFO)
data = extract_files.extract_json("Talent/10384.json")
pprint(data, sort_dicts=False)
si_columns = ["name", "date", "self_development", "geo_flex", "financial_support_self", "result"]
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
    pass


def convert_pi(info):
    """
    :param info: this will be a dictionary
    :return: will be dataframe
    """
    pass


def convert_weeks(info):
    """
    :param info: this will be a dataframe
    :return: should be dataframe
    """
    pass


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
                pass
    return pd.DataFrame(to_load_courses, index=[0])


pprint(convert_courses(data))
print(courses)


