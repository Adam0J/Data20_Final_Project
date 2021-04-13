from sqlalchemy import *
import pandas as pd
import extract_files
import boto3
from pprint import pprint
import re
import logging
import time

logging.basicConfig(level=logging.INFO)
pd.set_option("display.max_rows", None, "display.max_columns", None)
data = extract_files.extract_csv("Talent/April2019Applicants.csv")
# pprint(data, sort_dicts=False)
si_columns = ["name", "date", "self_development", "geo_flex", "financial_support_self", "result"]


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
    pass


def convert_courses(info):
    """
    probably won't need this
    :param info:
    :return:
    """
    pass


pprint(convert_pi(data))
