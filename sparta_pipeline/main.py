import pandas as pd
from sqlalchemy import *
from configparser import ConfigParser
from sparta_pipeline import transformations, extract_files
import re

# Creates all the required tables in a SQL Server database.
# creating_tables.main()

# Get all the keys in the s3 bucket.
transformations.sort_keys()

# All data from talent json files made into 7 separate dataframes.
talent_data = transformations.read_si()

# Creating 'Courses', 'Behaviour Scores', 'Behaviour Types', half of 'Staff information' table
behaviour_data = transformations.behaviour_tables()








#
#     df = pd.DataFrame(set(new_column), columns=[col])
#
# df = pd.concat(si).reset_index()
# df2 = pd.DataFrame(student_id, columns=["student_id"])
# output = pd.concat([df2, df], axis=1)
# print(output)

# transformations.applicants
# transformations.courses
# transformations.s_day