import pyodbc
from sqlalchemy import *
import pandas as pd

file = open("credentials.txt")
all_lines = file.readlines()
converted = []
for element in all_lines:
    converted.append(element.strip())

server = converted[0]
database = converted[1]
user = converted[2]
password = converted[3]
file.close()

driver = 'ODBC+Driver+17+for+SQL+Server'
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")


# open a connection using sqlalchemy
connection = engine.connect()

meta = MetaData()
students = Table(
    'students', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('lastname', String)
)
meta.create_all(engine)

engine.execute(students.insert(), [
    {'name': 'Matt', 'lastname': 'Whitaker'},
    {'name': 'Mark', 'lastname': 'Wu'}
])
data = engine.execute('SELECT * FROM students')
for i in data:
    print(i)

