import pandas as pd
from sqlalchemy import *
from configparser import ConfigParser
from sparta_pipeline import transformations, extract_files
# from sparta_pipeline import creating_tables
import re
from pprint import pprint

# Creates all the required tables in a SQL Server database.
# creating_tables.main()

# Get all the keys in the s3 bucket.
transformations.sort_keys()

# All data from talent json files made into 7 separate dataframes.
talent_data = transformations.read_si()

# All data from txt files made into 2 separate dataframes
sparta_scores = transformations.sparta_score_info()

# Output the Dataframe to load into sparta_day_information
transformations.gen_sparta(talent_data[0], sparta_scores[0])

# Output the Dataframe to load into the sparta_day_scores table
transformations.sparta_scores(sparta_scores[1], talent_data[7])

# Creating 'Courses', 'Behaviour Scores', 'Behaviour Types', half of 'Staff information' table
behaviour_data = transformations.behaviour_tables()

# Creating the contacts and personal info Dataframes
temp_pi = transformations.gen_pi()


# Creating the final personal info Dataframe
final_pi = transformations.final_pi(temp_pi[0], behaviour_data[1], behaviour_data[4], talent_data[7])

# list all of the dataframes to be loaded

tech_types =
strength_types =
weakness_types =
behaviour_types =
sparta_day_information =
staff_information =
personal_information =
contact_details =
courses =
behaviour_scores =
sparta_scores =
self_score =
student_strengths =
student_weaknesses =
