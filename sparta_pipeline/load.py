from sparta_pipeline import extract_files
from sparta_pipeline import transformations
from sqlalchemy import *
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

with open("credentials.txt") as f1, open("config.txt") as f2:
    line_file1 = f1.readlines()
    line_file2 = f2.readlines()
    converted = []
    for element in line_file1:
        converted.append(element.strip())
    for element in line_file2:
        converted.append(element.strip())

user = converted[0]
password = converted[1]
server = converted[2]
database = converted[3]
driver = converted[4]
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

connection = engine.connect()
meta = MetaData()


def load_courses_table():
    course = ['Business', 'Data', 'Engineering']
    df = pd.DataFrame(course, columns=['name'])
    logging.info(df)
    df.to_sql('courses', engine, index=False, if_exists="append")


def load_classes_table():
    pass


test = load_courses_table()
