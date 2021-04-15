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


def load_student_information():
    pass


def load_behaviours():
    pass


def load_weeks():
    pass


def load_techs():
    pass


def load_self_score():
    pass


def load_strengths():
    pass


def load_student_strengths():
    pass


def load_weaknesses():
    pass


def load_student_weaknesses():
    pass


def load_scores():
    pass


def load_personal_information():
    data = extract_files.extract_csv('Talent/July2019Applicants.csv')
    transformed_data = transformations.convert_pi(data)
    del transformed_data["id"]
   

def main():
    load_personal_information()


main()