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
driver = 'ODBC+Driver+17+for+SQL+Server'
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

connection = engine.connect()
meta = MetaData()


def create_student_information():

    students_information = Table(
        'students_information', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('date', Date),
        Column('self_development', Boolean),
        Column('geo_flex', Boolean),
        Column('financial_support_self', Boolean),
        Column('results', Boolean),
        Column('course_id', Integer),
        Column('course_code_id', Integer, ForeignKey("course_codes.course_id"))
    )
    meta.create_all(engine)
