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
pi = transformations.gen_pi()

# Creating the final personal info Dataframe
pprint(transformations.final_pi(pi[0], behaviour_data[3], talent_data[7]))
