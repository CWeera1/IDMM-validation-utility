
import os
from utils.IO_functions import read_csv_file, read_json_file
from utils.classes import Person, DatabaseConfig
import connectors.snowflake_connector

# define root directory
current_dir = os.getcwd()
root_dir = os.path.dirname(current_dir)

# read in the csv file using the util function and print to stdout

# Usage
file_path = 'tests/test.csv'
csv_data = read_csv_file(file_path)
print(csv_data)

# storing records in memory as python objects
person_list = []
names_list = []
for row in csv_data:
    person_list.append(row) # now person_list contains a list of each of the Person objects
print(person_list)

# read in the json file using the util function and print to stdout
file_path = 'config/csv_config.json'  # Specify the path to the JSON file
json_dict = read_json_file(file_path)  # Call the function to read and parse the JSON file
print(json_dict)

# create a cursor object to execute queries
cursor = conn.cursor()
