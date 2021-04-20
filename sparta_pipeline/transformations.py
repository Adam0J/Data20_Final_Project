from sqlalchemy import *
import pandas as pd
from sparta_pipeline.extract_files import *
import boto3
from pprint import pprint
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
    :param info: this will be a list
    :return: will be dataframe
    """
    new_list = [re.split(', | -  |: |/', i) for i in info]
    student_scores = []
    for student in new_list[3:]:
        full_name = student[0]
        psyc_score = int(student[2])
        psyc_max = int(student[3])
        pres_score = int(student[5])
        pres_max = int(student[6])
        student_scores.append([full_name, psyc_score, psyc_max, pres_score, pres_max])
    return pd.DataFrame(student_scores, columns=["full_name", "psychometrics_score", "psychometrics_max",
                                                 "presentations_score", "presentations_max"])


def date_fix(date_string):
    m = re.split(" ", date_string)
    if m[0] == "SEPT":
        return "SEPTEMBER" + " " + m[1]
    else:
        return date_string



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
    for i in range(len(temp_month)):
        temp_month[i] = date_fix(temp_month[i])
    temp_date = [datetime.strptime(x + " " + y, "%d %B %Y").date() if x + y != "NotInvited" else x + " " + y
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
    wks_col = list(
        itertools.chain.from_iterable(itertools.repeat(x, int((len(new_df)) / number_of_weeks)) for x in lst))

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

    id_name = pd.concat([output["student_id"], output["name"], output["date"]], axis=1)
    return output, tt_df, jt_df, st_df, js_df, wt_df, jw_df, id_name


def behaviour_tables():
    behaviour_scores = []
    course_names = []
    for key in courses:
        info = extract_csv(key)
        current_df = info.melt(id_vars=["name", "trainer"], var_name="behaviours", value_name="score")
        df2 = pd.DataFrame(current_df["behaviours"].str.split("_W").tolist(), columns=["behaviours", "week_id"])
        current_df["behaviours"] = df2["behaviours"]
        current_df["week_id"] = df2["week_id"]
        behaviour_scores.append(current_df)
        file_name_split = re.split("[/._]", key)[1:3]

        trainer = current_df["trainer"].iloc[0]  # Might be able to remove this as we already have a trainer df.
        course_names.append([file_name_split[0]+' '+file_name_split[1], trainer])

    bs_df = pd.concat(behaviour_scores)

    behaviour_types = bs_df["behaviours"].unique().tolist()
    bt_df = pd.DataFrame(behaviour_types, columns=["behaviour"])
    bt_df["behaviour_id"] = bt_df.index + 1
    bt_df = bt_df[["behaviour_id", "behaviour"]]
    bt_df = bt_df.astype({"behaviour_id": int})

    trainers = bs_df["trainer"].unique().tolist()
    del bs_df["trainer"]
    trainers_df = pd.DataFrame(trainers, columns=["full_name"])
    trainers_df["team"] = "trainer"
    trainers_df["staff_id"] = trainers_df.index + 1
    trainers_df = trainers_df[["staff_id", "full_name", "team"]]
    trainers_df = trainers_df.astype({"staff_id": int})

    bs_df = pd.merge(bs_df, bt_df, left_on="behaviours", right_on="behaviour", how="left")
    bs_df = bs_df.drop(["behaviours", "behaviour"], axis=1)
    bs_df = bs_df[["name", "week_id", "behaviour_id", "score"]]
    bs_df = bs_df.dropna()
    bs_df = bs_df.astype({"score": int, "week_id": int, "behaviour_id": int})
    bs_df = bs_df.sort_values(by=["name", "week_id"])

    course_df = pd.DataFrame(course_names, columns=["course_name", "staff_name"])
    course_df = pd.merge(course_df, trainers_df, left_on="staff_name", right_on="full_name", how="left")
    course_df = course_df.drop(["staff_name", "full_name", "team"], axis=1)
    course_df["course_id"] = course_df.index + 1
    course_df = course_df[["course_id", "course_name", "staff_id"]]

    return bs_df, trainers_df, bt_df, course_df


def read_sparta_day(key):
    file_contents = extract_txt(key)
    names = [re.split(" - ", i)[0] for i in file_contents[3:]]
    name_df = pd.DataFrame(names, columns=["full_name"])
    name_df["location"] = file_contents[1]
    return name_df, convert_scores(file_contents)


def sparta_score_info():
    locations = []
    scores = []
    for i in s_day:
        current = read_sparta_day(i)
        locations.append(current[0])
        scores.append(current[1])
    return pd.concat(locations), pd.concat(scores)


def gen_sparta(input_df, loc_info):
    final_sparta = pd.merge(input_df, loc_info, left_on=input_df["name"].str.lower(),
                            right_on=loc_info["full_name"].str.lower(), how="inner")
    del final_sparta["name"]
    del final_sparta["key_0"]
    del final_sparta["full_name"]
    return final_sparta


def sparta_scores(input_df, id_df):
    final_score = pd.merge(id_df, input_df, left_on=id_df["name"].str.lower(),
                           right_on=input_df["full_name"].str.lower(), how="inner")
    del final_score["key_0"]
    del final_score["name"]
    del final_score["date"]
    del final_score["full_name"]
    return final_score


def gen_pi():
    pi_list = []
    contacts_list = []
    for i in applicants:
        d = extract_csv(i)
        total = convert_pi(d)
        pi_list.append(total[0])
        contacts_list.append(total[1])
    pi = pd.concat(pi_list)
    contacts = pd.concat(contacts_list)

    return pi, contacts


def final_pi(input_df, staff_id_df, course_id_df, student_id_df):
    pass
