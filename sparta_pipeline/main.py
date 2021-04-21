from sparta_pipeline import creating_tables, transformations, load
import time
import re

# Creates all the required tables in a SQL Server database.
creating_tables.main() # fix this

# Get all the keys in the s3 bucket.
transformations.sort_keys()

# All data from talent json files made into 8 separate dataframes.
talent_data = transformations.read_si()

# All data from txt files made into 2 separate dataframes
s_scores = transformations.sparta_score_info()

# Output the Dataframe to load into sparta_day_information
sdi_df = transformations.gen_sparta(talent_data[0], s_scores[0])

# Output the Dataframe to load into the sparta_day_scores table
sds_df = transformations.sparta_scores(s_scores[1], talent_data[7])

# Creating 'Behaviour Scores' df, Half of 'Staff information' df, 'Behaviour Types' df, Courses df
behaviour_data = transformations.behaviour_tables()
bs_df = transformations.behaviour_scores(behaviour_data[0], talent_data[7])

bt_df = behaviour_data[2]

# Creating the contacts and personal info Dataframes
temp_pi = transformations.gen_pi(talent_data[7])

# Creating the final personal info Dataframe
final_personal_info = transformations.final_pi(temp_pi[0], behaviour_data[1], behaviour_data[4])

# list all of the dataframes to be loaded

tech_types = talent_data[1]
strength_types = talent_data[3]
weakness_types = talent_data[5]
behaviour_types = behaviour_data[2]
sparta_day_information = sdi_df
staff_information = final_personal_info[1]
courses = behaviour_data[3]
personal_information = final_personal_info[0]
contact_details = temp_pi[1]
behaviour_scores = bs_df
sparta_day_scores = sds_df
self_score = talent_data[2]
student_strengths = talent_data[4]
student_weaknesses = talent_data[6]


# Examples of loading some tables.
# load.load(tech_types, 'tech_types')
# load.load(strength_types, 'strength_types')
# load.load(weakness_types, 'weakness_types')
# load.load(behaviour_types, 'behaviour_types')
# load.load(sparta_day_information, 'sparta_day_information')
# load.load(staff_information, 'staff_information')
# load.load(courses, 'courses')
#
#
# load.load(personal_information, 'personal_information')



