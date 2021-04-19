from sqlalchemy import *
import pandas as pd
from sparta_pipeline.extract_files import *
import boto3
from pprint import pprint
import re
import logging
import time
import itertools
import re
from datetime import datetime

# data = extract_files.extract_json("Talent/10384.json")
# dataCsv = extract_files.extract_csv("Academy/Data_28_2019-02-18.csv")
# data_app = 'Talent/Feb2019Applicants.csv'
logging.basicConfig(level=logging.INFO)

pd.set_option("display.max_rows", None, "display.max_columns", None)


si_columns = ["name", "date", "self_development", "geo_flex", "financial_support_self", "result", "course_interest"]
weeks_columns = ["student_id", "week_id", "behaviour_id", "score"]
courses_column = "name"


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
    info.rename(columns={"name": "full_name"}, inplace=True)

    contact_df = info[["email", "city", "address", "postcode", "phone_number"]].copy()
    info.drop(["email", "city", "address", "postcode", "phone_number"], axis=1, inplace=True)
    return info, contact_df


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
    courses = []
    for entry in info:
        if entry == "course_interest":
            # only add course if it's not already been added
            if str(info[entry]) not in courses:
                to_load_courses.update({courses_column: info[entry]})
                courses.append(info[entry])
            else:
                continue
    return pd.DataFrame(to_load_courses, index=[0])


def convert_tech_types():
    to_load_tech_types = []
    data = extract_json('Talent/10385.json')
    for entry in data:
        if entry == 'tech_self_score':
            if data[entry] not in to_load_tech_types:
                to_load_tech_types.extend(data[entry])
    print(to_load_tech_types)


def get_unique_column_csv(col, csv_keys):
    new_column = []
    for key in csv_keys:
        file = extract_csv(key)
        value = file.dropna()
        new_column.extend(value[col].unique().tolist())

    return set(new_column)


def get_unique_column_json(col, json_keys):
    new_column = []
    for key in json_keys:
        file = extract_json(key)
        value = file.get(col)
        if value:
            new_column.extend(value)
    df = pd.DataFrame(set(new_column), columns=[col])
    return df


def sparta_location(key):
    file_contents = extract_txt(key)
    names = [re.split(" - ", i)[0] for i in file_contents[3:]]
    name_df = pd.DataFrame(names, columns=["full_name"])
    name_df["location"] = file_contents[1]
    return name_df


def course_trainers():
    list_courses = []
    trainers = []
    for key in courses:
        trainer = extract_csv(key)["trainer"].unique().tolist()[0]
        temp = key[8:-15].split('_')
        course_code = temp[0] + ' ' + temp[1]

        list_courses.append([course_code, trainer])
        trainers.append(trainer)

    df = pd.DataFrame(list_courses, columns=['course_name', 'trainer'])

    return df, set(trainers)


def convert_staff_information():
    talent_names = get_unique_column_csv('invited_by', applicants)
    trainer_names = course_trainers()[1]
    df = pd.DataFrame(talent_names, columns=['full_name'])
    df["team"] = "Talent"
    df2 = pd.DataFrame(trainer_names, columns=['full_name'])
    df2["team"] = "Trainer"

    final_df = pd.concat([df, df2]).reset_index()
    final_df["index"] = final_df.index + 1

    return final_df


def staff_to_ids():
    """
    Changes the convert_statt_information dataframe from 'full_name' and 'team' to 'full_name', 'team' and 'staff_id'
    based on the staff table.
    """
    staff_info_df = convert_staff_information()
    course_info_df = course_trainers()[0]
    staff_course = pd.merge(staff_info_df, course_info_df, left_on="full_name", right_on="trainer", how="right")

    staff_course.drop(["full_name", "team", "trainer"], axis=1, inplace=True)
    staff_course.rename(columns={"index": "staff_id"}, inplace=True)

    logging.info(staff_course)


def get_list_types(sid, input_list, output, join_table):
    for i in input_list:
        if i not in output:
            output.append(i)
            join_table.append([sid, output.index(i) + 1])
        else:
            join_table.append([sid, output.index(i) + 1])


def get_dict_types(sid, input_dict, output, join_table):
    for i in list(input_dict.keys()):
        if i not in output:
            output.append(i)
            join_table.append([sid, output.index(i) + 1, input_dict[i]])

        else:
            join_table.append([sid, output.index(i) + 1, input_dict[i]])


def read_si():
    student_id = []
    si = []
    tech_types = []
    join_tech = []
    strength_types = []
    join_strengths = []
    weakness_types = []
    join_weaknesses = []

    for key in students[1:20]:
        file = extract_json(key)
        si.append(convert_si(file))
        s_id = re.split("[/.]", key)[1]
        student_id.append(s_id)

        tech = file.get("tech_self_score")
        if tech:
            get_dict_types(s_id, tech, tech_types, join_tech)

        strengths = file.get("strengths")
        if strengths:
            get_list_types(s_id, strengths, strength_types, join_strengths)

        weaknesses = file.get("weaknesses")
        if weaknesses:
            get_list_types(s_id, weaknesses, weakness_types, join_weaknesses)

    df = pd.concat(si).reset_index()
    df2 = pd.DataFrame(student_id, columns=["student_id"])
    output = pd.concat([df2, df], axis=1)
    del output["index"]

    tt_df = pd.DataFrame(tech_types, columns=["tech_name"])
    jt_df = pd.DataFrame(join_tech, columns=["student_id", "tech_id", "tech_self_score"])

    st_df = pd.DataFrame(strength_types, columns=["strength_name"])
    js_df = pd.DataFrame(join_strengths, columns=["student_id", "strength_id"])

    wt_df = pd.DataFrame(weakness_types, columns=["weakness_name"])
    jw_df = pd.DataFrame(join_weaknesses, columns=["student_id", "weakness_id"])

    return output, tt_df, jt_df, st_df, js_df, wt_df, jw_df
