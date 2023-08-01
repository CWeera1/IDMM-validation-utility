import os
import json
import pandas as pd
from connectors.snowflake_connector import SnowflakeConnector
from utils.validation_functions import CSVValidator  # Import the class instead of individual functions
from utils.API_interaction import API_Interface, CSV_Config_Manager


def load_configs():
    # define root directory
    root_dir = os.getcwd()

    # generate path to config.json files
    csv_config_path = os.path.join(root_dir, 'config', 'csv_config.json')
    db_config_path = os.path.join(root_dir, 'config', 'db_config.json')

    # Load json config
    with open(csv_config_path, 'r') as f:
        csv_config_json = json.load(f)

    # Load the database configuration from db_config.json
    with open(db_config_path, 'r') as f:
        db_config = json.load(f)

    # Get csv file path from json config
    csv_file_path = os.path.join(root_dir, 'tests', csv_config_json['source_file_name'])

    # Load csv data
    csv_data = pd.read_csv(csv_file_path)

    return csv_data, csv_config_json, db_config, csv_file_path

def validate_data(csv_data, csv_config):
    validator = CSVValidator(csv_config)  # Create an instance of the CSVValidator class
    if not validator.validate_csv(csv_data):  # Use the class method to validate the CSV
        print("Validation failed. The CSV file does not meet the specified criteria.")
        exit(1)

    print("CSV file validated successfully.")
    return csv_data

def migrate_data(cleaned_data, db_config, connector, csv_file_path, csv_config):
    # Extract the table name from the csv file name
    table_name = os.path.basename(csv_file_path).replace('.csv', '').upper()

    # Extract the schema from the db_config
    db_schema = db_config['database_details']['schema'] # 'PUBLIC' by default

    # Generate the Snowflake table definition based on the CSV config
    table_definition = connector.create_table_definition(csv_config)

    # Copy the data and uppercase the headings in preparation for uploading
    cleaned_data.columns = cleaned_data.columns.str.upper()

    # Implement the refresh_table function - if table exists, refresh data, else create table and append data
    connector.refresh_table(table_name, table_definition, cleaned_data, db_schema)

def main():
    
    
    print('running main function')

    print('instantiating classes')

    response_file_path = "config/response.json"
    config_file_path = "config/csv_config.json"
    csv_file_path = "tests/opals_test.csv"
    api_interactor = API_Interface(response_file_path, csv_file_path)
    csv_config_manager = CSV_Config_Manager(config_file_path, response_file_path)

    print('Connecting to API to update response.json')
    # TODO: connect to API once access is provided and write over response.json
    #api_instance = API_Interface(response_file_path, csv_file_path)
    #api_column_data_types = api_instance.get_column_data_types_from_api(auth_token_example)
    print('API inactive - response.json unchanged')
    
    print('updating csv_config')
    csv_config_manager.load_config()
    csv_config_manager.update_csv_config()
    print('csv config updated')

    print('loading configs')
    csv_data, csv_config, db_config, csv_file_path = load_configs()
    print(f'configs loaded: {csv_data}')
    
    validated_data = validate_data(csv_data, csv_config)

    # Create an instance of the CSVValidator class and call the clean_data method
    validator = CSVValidator(csv_config)
    cleaned_data = validator.clean_data(validated_data)


    # Use SnowflakeConnector for data migration
    connector = SnowflakeConnector(db_config['database_details'])
    connector.connect()
    connector.setup_snowflake(connector.warehouse, connector.database, connector.schema)
    
    migrate_data(cleaned_data, db_config, connector, csv_file_path, csv_config)
    connector.close()

if __name__ == "__main__":
    main()
