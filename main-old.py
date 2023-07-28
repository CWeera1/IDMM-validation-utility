import os
import json
from utils.validation_functions import validate_csv, clean_csv
import pandas as pd

import connectors.snowflake_connector

# define root directory
root_dir = os.getcwd()

# generate path to config.json files
csv_config_path = os.path.join(root_dir, 'config', 'csv_config.json')
db_config_path = os.path.join(root_dir, 'config', 'db_config.json')

# Load json config
with open(csv_config_path, 'r') as f:
    csv_config_json = json.load(f)

# Get csv file path from json config
csv_file_path = os.path.join(root_dir, 'tests', csv_config_json['source_file_name'])

# Load csv data
csv_data = pd.read_csv(csv_file_path)

# validate the csv file
if not validate_csv(csv_data, csv_config_json):
    print("Validation failed. The CSV file does not meet the specified criteria.")
    exit(1)

# If csv passes validation, continue with rest of the program
print("CSV file validated successfully.")

# Clean the csv_file for any formatting incompatibilities between python and SQL
csv_data = clean_csv(csv_data, csv_config_json)

# Load the database configuration from db_config.json
with open(db_config_path, 'r') as f:
    db_config = json.load(f)

# Extract the table name from the csv file name
table_name = os.path.basename(csv_file_path).replace('.csv', '').upper()

# Extract the schema from the db_config
db_schema = db_config['database_details']['schema'] # 'PUBLIC' by default

# Generate the Snowflake table definition based on the CSV config
table_definition = connectors.snowflake_connector.create_table_definition(csv_config_json)
print(f"Generated table definition: {table_definition}")

# Check if the database type is Snowflake
if db_config['database_type'].lower() == 'snowflake':
    # Extract Snowflake details
    snowflake_config = db_config['database_details']
    
    # Establish a connection to Snowflake
    conn = connectors.snowflake_connector.connect_to_snowflake(snowflake_config)

    # Perform setup for Snowflake by creating and using warehouse, database, and schema
    connectors.snowflake_connector.setup_snowflake(conn, snowflake_config['warehouse'], snowflake_config['database'], db_schema)

    # Copy the data and uppercase the headings in preparation for uploading
    csv_data_upper = csv_data.copy()
    csv_data_upper.columns = csv_data_upper.columns.str.upper()

    # Implement the refresh_table function - if table exists, refresh data, else create table and append data
    connectors.snowflake_connector.refresh_table(conn, table_name, table_definition, csv_data_upper, db_schema)

    # Close the connection to Snowflake
    connectors.snowflake_connector.close_connection(conn)