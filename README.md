# Data20_Final_Project

Hello!

In this ReadMe, we will detail what can be found in this repository, how to run
the files inside, and what software and permissions you will need to run everything 
contained within.

## What is the aim of this project?

Simply, this project accesses a particular bucket on Amazon S3's Web Servers. This 
bucket contains (fake) information relating to Sparta Global applicants, including 
interview details, "Sparta Day" events, contact details. CSV files
relating to course information contain the names of successful applicants, the name 
of their trainer and details on each individual's "Sparta Behaviours" scores, across
the weeks of the course. 

The files and functions contained within access this data and separate it according to
multiple parameters, and load this data into multiple tables. These tables include, 
but are not limited to:
* Student Strengths and Weaknesses
* Behaviour Scores per week 
* Student Information 
* Student Contact Information 

Once the data from Amazon S3 has been loaded into tables, it should be suitable 
for Data Analysts to study, and draw conclusions from the data provided.

## Sounds great! How do I run these files?

This project makes use of Microsoft Azure Data Studio to access tables that are
created within the functions of this project. If you wish to run this project,
you will need to have access to Azure! 

*in the future, possibly at the end of the project, the config file will be pushed* 
*for now we will only reference the credentials file*

The project files are already pointing to the necessary parameters for connecting
to Azure, but you must create a .txt file locally to be accessed by the project
files. Inside the sparta_pipeline directory, create "credentials.txt"

On the first line, input your Username, and on the second line, input your password.
This is all you need to add for the project files to access your Azure credentials! 

Once you have Azure set up, as well as your personal credentials file, you are ready
to run the project!

## So, what does this project *do* exactly?

At the moment, there are four primary files in the "sparta_pipeline" directory:
1. creating_tables.py
2. extract_files.py
3. transformations.py
4. load.py

To begin, run the first file in this list; this creates the tables on Azure that
the extracted and transformed data will be loaded into. You must make sure you
have a connection to Azure at this point, otherwise you will experience connection
errors! 

When you run extract_files.py, a connection will be made to Amazon S3, that will 
extract all files from the Amazon S3 bucket titled "data20-final-project". 
Once all project files are functionally linked together, the outputs from this 
file will move to the next file in the list.

As the name suggests, transformations.py takes the extracted data and performs the 
necessary transformations to ensure the data is formatted properly, so that it
can be loaded into the tables created in step 1. This is the most computationally 
intensive step.

Once the transformations have been completed on all files, the last step is to load 
the data into the tables created on Azure. Once again, this creates a connection 
with Azure, and the data is automatically loaded into the tables. This data
is then suitable for querying via SQL. Note that Azure uses SQL Server syntax, 
if you'd like to do some querying of your own!