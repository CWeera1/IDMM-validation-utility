import os
import json
from utils.classes import DatabaseConfig
from utils.validation_functions import validate_csv
import pandas as pd

import connectors.snowflake_connector

# define root directory
root_dir = os.getcwd()

# generate path to csv_config.json
csv_config_path = os.path.join(root_dir, 'config', 'csv_config.json')

# Load json config
with open(csv_config_path, 'r') as f:
    json_config = json.load(f)

# Get csv file path from json config
csv_file_path = os.path.join(root_dir, 'tests', json_config['source_file_name'])

# Load csv data
csv_data = pd.read_csv(csv_file_path)

# validate the csv file
if not validate_csv(csv_data, json_config):
    print("Validation failed. The CSV file does not meet the specified criteria.")
    exit(1)

# If csv passes validation, continue with rest of the program
print("CSV file validated successfully.")
