import pyodbc
from sqlalchemy import *
import pandas as pd

file = open("credentials.txt")
all_lines = file.readlines()
converted = []
for element in all_lines:
    converted.append(element.strip())
file.close()

server = converted[0]
database = converted[1]
user = converted[2]
password = converted[3]
driver = converted[4]
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

connection = engine.connect()
meta = MetaData()


def create_weaknesses():
    Table(
        'weaknesses', meta,
        Column('weakness_id', Integer, primary_key=True),
        Column('name', String)
    )


def create_student_weaknesses():
    Table(
        'student_weaknesses', meta,
        Column('student_id', ForeignKey("student_information.student_id")),
        Column('weakness_id', ForeignKey("weaknesses.weakness_id"))
    )


def create_behaviours():
    Table(
        'behaviours', meta,
        Column('behaviour_ID', Integer, primary_key=True),
        Column('behaviour', String)
    )


def create_weeks():
    Table(
        'weeks', meta,
        Column('student_id', Integer, ForeignKey("students_information.id")),
        Column('week_id', Integer),
        Column('behaviour_id', Integer, ForeignKey("behaviours.behaviour_ID")),
        Column('score', Integer)
    )


def create_techs():
    Table(
        'techs', meta,
        Column('tech_id', Integer, primary_key=True),
        Column('name', String)
    )


def create_self_score():
    Table(
        'self_score', meta,
        Column('student_id', Integer, ForeignKey("students_information.id")),
        Column('tech_id', Integer, ForeignKey("techs.tech_id"))
    )


def create_student_information():
    Table(
        'students_information', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('date', Date),
        Column('self_development', Boolean),
        Column('geo_flex', Boolean),
        Column('financial_support_self', Boolean),
        Column('results', Boolean),
        Column('course_id', Integer, ForeignKey("courses.id")),
        Column('course_code_id', Integer, ForeignKey("course_codes.course_code_id"))
    )


def create_strengths():
    Table(
        'strengths', meta,
        Column('strength_id', Integer, primary_key=True),
        Column('name', String)
    )


def create_student_strengths():
    Table(
        'student_strengths', meta,
        Column('student_id', Integer, ForeignKey("student_information.id")),
        Column('strength_id', Integer, ForeignKey("strengths.strength_id"))
    )


def create_courses():
    Table(
        'courses', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String),
    )


def create_course_id():
    Table(
        'course_codes', meta,
        Column('course_code_id', Integer, primary_key=True),
        Column('name_number', String),
    )


meta.create_all(engine)

