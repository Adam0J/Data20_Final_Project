from sparta_pipeline.extract_files import *
import logging
import re
from datetime import datetime
import numpy as np

logging.basicConfig(level=logging.INFO)

pd.set_option("display.max_rows", None, "display.max_columns", None)

si_columns = ["name", "date", "self_development", "geo_flex", "financial_support_self", "result", "course_interest"]

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
            elif "//" in info.get(i):
                temp = info.get(i).replace("//", "/")
                temp = datetime.strptime(temp, "%d/%m/%Y").date()
                to_load.update({i: temp})
            elif "/" in info.get(i):
                to_load.update({i: datetime.strptime(info.get(i), "%d/%m/%Y").date()})
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
    info["dob"] = pd.to_datetime(info["dob"])
    info["invited_date"] = info["invited_date"].replace("Not Invited", np.nan)

    return info


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
            if input_dict[i] > 10:
                join_table.append([sid, output.index(i) + 1, 10])
            else:
                join_table.append([sid, output.index(i) + 1, input_dict[i]])

        else:
            if input_dict[i] > 10:
                join_table.append([sid, output.index(i) + 1, 10])
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

    for key in students:
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
    df2 = pd.DataFrame(student_id, columns=["student_id"], dtype=int)
    output = pd.concat([df2, df], axis=1)
    output["date"] = pd.to_datetime(output["date"])
    del output["index"]

    tt_df = pd.DataFrame(tech_types, columns=["tech_name"])
    jt_df = pd.DataFrame(join_tech, columns=["student_id", "tech_id", "tech_self_score"])

    st_df = pd.DataFrame(strength_types, columns=["strength"])
    js_df = pd.DataFrame(join_strengths, columns=["student_id", "strength_id"])

    wt_df = pd.DataFrame(weakness_types, columns=["weakness"])
    jw_df = pd.DataFrame(join_weaknesses, columns=["student_id", "weakness_id"])

    id_name = pd.concat([output["student_id"], output["name"], output["date"]], axis=1)


    return output, tt_df, jt_df, st_df, js_df, wt_df, jw_df, id_name


def behaviour_tables():
    beh_scores = []
    course_names = []
    all_students = []
    for key in courses:
        info = extract_csv(key)
        current_df = info.melt(id_vars=["name", "trainer"], var_name="behaviours", value_name="score")
        df2 = pd.DataFrame(current_df["behaviours"].str.split("_W").tolist(), columns=["behaviours", "week_id"])
        current_df["behaviours"] = df2["behaviours"]
        current_df["week_id"] = df2["week_id"]
        beh_scores.append(current_df)
        file_name_split = re.split("[/._]", key)[1:3]

        trainer = current_df["trainer"].iloc[0]  # Might be able to remove this as we already have a trainer df.
        course = file_name_split[0] + ' ' + file_name_split[1]
        course_students = info["name"]
        course_students = course_students.to_frame()
        course_students["c"] = course
        all_students.append(course_students)
        course_names.append([course, trainer])

    bs_df = pd.concat(beh_scores)

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
    bs_df.rename(columns={"score": "behaviour_score"}, inplace=True)
    bs_df = bs_df.dropna()
    bs_df = bs_df.astype({"behaviour_score": int, "week_id": int, "behaviour_id": int})
    bs_df = bs_df.sort_values(by=["name", "week_id"])

    course_df = pd.DataFrame(course_names, columns=["course_name", "staff_name"])
    course_df = pd.merge(course_df, trainers_df, left_on="staff_name", right_on="full_name", how="left")
    course_df = course_df.drop(["staff_name", "full_name", "team"], axis=1)
    course_df["course_id"] = course_df.index + 1
    course_df = course_df[["course_id", "course_name", "staff_id"]]

    student_course_mid = pd.concat(all_students)
    student_course = pd.merge(student_course_mid, course_df, left_on=student_course_mid["c"],
                              right_on=course_df["course_name"], how="inner")
    student_course = student_course.drop(["key_0", "c", "course_name", "staff_id"], axis=1)

    return bs_df, trainers_df, bt_df, course_df, student_course


def read_sparta_day(key):
    file_contents = extract_txt(key)
    names = [re.split(" - ", i)[0] for i in file_contents[3:]]
    name_df = pd.DataFrame(names, columns=["full_name"])
    name_df["location"] = file_contents[1]
    name_df["sparta_day_date"] = file_contents[0]
    name_df["sparta_day_date"] = pd.to_datetime(name_df["sparta_day_date"], format="%A %d %B %Y")
    score_df = convert_scores(file_contents)
    score_df["sparta_day_date"] = file_contents[0]
    score_df["sparta_day_date"] = pd.to_datetime(score_df["sparta_day_date"], format="%A %d %B %Y")

    return name_df, score_df


def sparta_score_info():
    locations = []
    scores = []
    for i in s_day:
        current = read_sparta_day(i)
        locations.append(current[0])
        scores.append(current[1])
    loc = pd.concat(locations)
    sc = pd.concat(scores)

    sc["full_name"] = sc["full_name"].str.replace("[;.]| '", "", regex=True)
    sc["full_name"] = sc["full_name"].str.replace("' ", "'", regex=True)
    sc["full_name"] = sc["full_name"].str.replace(" - ", "-", regex=True)
    sc["full_name"] = sc["full_name"].str.replace("\AD'", "D", regex=True)

    return loc, sc


def gen_sparta(input_df, loc_info):
    final_sparta = pd.merge(input_df, loc_info, left_on=[input_df["name"].str.lower(), input_df["date"]],
                            right_on=[loc_info["full_name"].str.lower(), loc_info["sparta_day_date"]], how="inner")

    final_sparta = final_sparta.drop_duplicates(subset=["student_id"])

    final_sparta.drop(["name", "key_0", "key_1", "full_name", "sparta_day_date"], axis=1, inplace=True)
    final_sparta.rename(columns={"date": "invited_date", "result": "passed"}, inplace=True)

    return final_sparta


def sparta_scores(input_df, id_df):
    final_score = pd.merge(id_df, input_df, left_on=[id_df["name"].str.lower(), id_df["date"]],
                           right_on=[input_df["full_name"].str.lower(), input_df["sparta_day_date"]], how="inner")
    final_score = final_score.drop(["key_0", "key_1", "name", "date", "full_name", "sparta_day_date"], axis=1)

    return final_score


def gen_pi(student_id_df):
    pi_list = []
    for i in applicants:
        d = extract_csv(i)
        total = convert_pi(d)
        pi_list.append(total)
    pi = pd.concat(pi_list)
    pi["invited_date"] = pd.to_datetime(pi["invited_date"])

    new_pi = pd.merge(student_id_df, pi, left_on=[student_id_df["name"].str.lower(),
                                                  student_id_df["date"]],
                      right_on=[pi["full_name"].str.lower(), pi["invited_date"]], how="right")
    new_pi.drop(["key_0", "key_1", "date", "id", "name"], axis=1, inplace=True)

    contacts = new_pi[["student_id", "email", "city", "address", "postcode", "phone_number"]].copy()
    new_pi.drop(["email", "city", "address", "postcode", "phone_number"], axis=1, inplace=True)
    index_series = pd.Series([i for i in range(1, len(new_pi)+1)])
    new_pi["student_id"].fillna(value=index_series, inplace=True)
    new_pi = new_pi.astype({"student_id": int})
    contacts = contacts.drop_duplicates(subset=contacts.columns.difference(["student_id"]))

    contacts["address"] = contacts["address"].str.title()

    return new_pi, contacts


def final_pi(input_df, staff_id_df, course_id_df):

    trainers = input_df["invited_by"].unique().tolist()
    trainers_df = pd.DataFrame(trainers, columns=["full_name"])
    trainers_df["team"] = "talent"
    staff = pd.concat([staff_id_df, trainers_df]).reset_index()
    staff["staff_id"] = staff.index + 1
    del staff["index"]

    with_tid = pd.merge(input_df, staff, left_on=input_df["invited_by"],
                        right_on=staff["full_name"], how="inner")
    with_tid = with_tid.drop(["key_0", "invited_by", "full_name_y", "team"], axis=1)

    final = pd.merge(with_tid, course_id_df, left_on=with_tid["full_name_x"].str.lower(),
                     right_on=course_id_df["name"].str.lower(), how="inner")
    final.drop(["key_0", "invited_date", "name"], axis=1, inplace=True)
    final.rename(columns={"full_name_x": "full_name"}, inplace=True)

    final = final.drop_duplicates(subset=final.columns.difference(["student_id"]))

    final["full_name"] = final["full_name"].str.title()

    return final, staff


def behaviour_scores(input_df, id_df):
    behaviour_scores_df = pd.merge(id_df, input_df, left_on=id_df["name"].str.lower(),
                                   right_on=input_df["name"].str.lower(), how="inner")

    behaviour_scores_df = behaviour_scores_df.drop(["key_0", "name_x", "date", "name_y"], axis=1)

    return behaviour_scores_df
