from sparta_pipeline import creating_tables, transformations, load

# Creates all the required tables in a SQL Server database.
creating_tables.main()

# Get all the keys in the s3 bucket.
transformations.sort_keys()

# All data from talent json files made into 8 separate dataframes.
talent_data = transformations.read_si()

# All data from txt files made into 2 separate dataframes
sparta_scores = transformations.sparta_score_info()

# Output the Dataframe to load into sparta_day_information
sdi_df = transformations.gen_sparta(talent_data[0], sparta_scores[0])

# Output the Dataframe to load into the sparta_day_scores table
transformations.sparta_scores(sparta_scores[1], talent_data[7])

# Creating 'Behaviour Scores' df, Half of 'Staff information' df, 'Behaviour Types' df, Courses df
behaviour_data = transformations.behaviour_tables()
bs_df = behaviour_data[0]
bs_df = transformations.behaviour_scores(bs_df, talent_data[7])

bt_df = behaviour_data[2]

# Creating the contacts and personal info Dataframes
pi = transformations.gen_pi()


# Examples of loading some tables.
load.load(bt_df, 'behaviour_types', False)
load.load(sdi_df, 'sparta_day_information', False)
load.load(bs_df, 'behaviour_scores', False)






