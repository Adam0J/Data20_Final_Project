import pandas as pd
from sqlalchemy import *
from configparser import ConfigParser

# Read config.ini file
config_object = ConfigParser()
config_object.read("../config.ini")

# Get the userinfo
userinfo = config_object["USERINFO"]

with open("../credentials.txt") as f1:
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


def create_staff():
    Table(
        "staff_information", meta,
        Column("staff_id", Integer, primary_key=True, autoincrement=False),
        Column("full_name", String),
        Column("team", String)
    )


def create_contacts():
    Table(
        "contact_details", meta,
        Column("student_id", Integer, ForeignKey("personal_information.student_id"), primary_key=True),
        Column('email', String),
        Column('city', String),
        Column('address', String),
        Column('postcode', String),
        Column('phone_number', String)
    )


def create_courses():
    Table(
        'courses', meta,
        Column('course_id', Integer, primary_key=True),
        Column('course_name', String),
        Column("staff_id", Integer, ForeignKey("staff_information.staff_id"))
    )


def create_sparta():
    Table(
        'sparta_day_information', meta,
        Column('student_id', Integer, primary_key=True, autoincrement=False),
        Column('invited_date', Date),
        Column('self_development', Boolean),
        Column('geo_flex', Boolean),
        Column('financial_support_self', Boolean),
        Column('passed', Boolean),
        Column('course_interest', String),
        Column('location', String)
    )


def create_behaviours():
    Table(
        'behaviour_types', meta,
        Column('behaviour_id', Integer, primary_key=True),
        Column('behaviour', String)
    )


def create_behaviour_scores():
    Table(
        'behaviour_scores', meta,
        Column('student_id', Integer, ForeignKey("sparta_day_information.student_id")),
        Column('week_id', Integer),
        Column('behaviour_id', Integer, ForeignKey("behaviour_types.behaviour_id")),
        Column('behaviour_score', Integer)
    )


def create_techs():
    Table(
        'tech_types', meta,
        Column('tech_id', Integer, primary_key=True),
        Column('tech_name', String)
    )


def create_self_score():
    Table(
        'self_score', meta,
        Column('student_id', Integer, ForeignKey("sparta_day_information.student_id")),
        Column('tech_id', Integer, ForeignKey("tech_types.tech_id")),
        Column('tech_self_score', Integer)
    )


def create_strengths():
    Table(
        'strength_types', meta,
        Column('strength_id', Integer, primary_key=True),
        Column('strength', String)
    )


def create_student_strengths():
    Table(
        'student_strengths', meta,
        Column('student_id', Integer, ForeignKey("sparta_day_information.student_id")),
        Column('strength_id', Integer, ForeignKey("strength_types.strength_id"))
    )


def create_weaknesses():
    Table(
        'weakness_types', meta,
        Column('weakness_id', Integer, primary_key=True),
        Column('weakness', String)
    )


def create_student_weaknesses():
    Table(
        'student_weaknesses', meta,
        Column('student_id', ForeignKey("sparta_day_information.student_id")),
        Column('weakness_id', ForeignKey("weakness_types.weakness_id"))
    )


def create_sparta_scores():
    Table(
        'sparta_day_scores', meta,
        Column('student_id', Integer, ForeignKey("sparta_day_information.student_id"), primary_key=True),
        Column('psychometrics_score', Integer),
        Column('psychometrics_max', Integer),
        Column('presentations_score', Integer),
        Column('presentations_max', Integer)
    )


def create_personal_information():
    Table(
        'personal_information', meta,
        Column('student_id', Integer, ForeignKey("sparta_day_information.student_id"), primary_key=True),
        Column('full_name', String),
        Column('gender', String),
        Column('dob', Date),
        Column('uni', String),
        Column('degree', String),
        Column('staff_id', Integer, ForeignKey("staff_information.staff_id")),
        Column('course_id', Integer, ForeignKey("courses.course_id")),
    )


def main():
    create_staff()
    create_courses()
    create_sparta()
    create_behaviours()
    create_behaviour_scores()
    create_techs()
    create_self_score()
    create_strengths()
    create_student_strengths()
    create_weaknesses()
    create_student_weaknesses()
    create_sparta_scores()
    create_personal_information()
    create_contacts()
    meta.create_all(engine)


if __name__ == "__main__":
    main()

