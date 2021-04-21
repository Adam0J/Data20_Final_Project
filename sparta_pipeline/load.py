from sparta_pipeline.config_manager import *
from sqlalchemy import *

with open("..\\credentials.txt") as f1:
    line_file1 = f1.readlines()
    converted = []
    for element in line_file1:
        converted.append(element.strip())

user = converted[0]
password = converted[1]

engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server()}/{database()}?driver={driver()}")
connection = engine.connect()
meta = MetaData()


def load(df, input_table):
    df.to_sql(input_table, engine, index=False, if_exists="append")


