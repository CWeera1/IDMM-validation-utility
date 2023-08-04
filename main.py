import os
import json
import pandas as pd
from utils.validation_functions.validation_controller import validation_controller  # Import the class instead of individual functions
from utils.API_interactions.API_interaction import API_Interface, CSV_Config_Manager

def load_configs():
    # define root directory
    root_dir = os.getcwd()

    # generate path to config.json files
    csv_config_path = os.path.join(root_dir, 'config', 'csv_config.json')

    # Load json config
    with open(csv_config_path, 'r') as f:
        csv_config_json = json.load(f)

    # Get csv file path from json config
    csv_file_path = os.path.join(root_dir, 'tests', csv_config_json['source_file_name'])

    # Load csv data
    csv_data = pd.read_csv(csv_file_path, delimiter=csv_config_json['expected_delimiter'])

    return csv_data, csv_config_json, csv_file_path

def validate_data(csv_data, csv_config):
    validator = validation_controller(csv_config)  # Create an instance of the validation_controller class
    if not validator.validate_csv(csv_data):  # Use the class method to validate the CSV
        print("Validation failed. The CSV file does not meet the specified criteria.")
        exit(1)

    print("CSV file validated successfully.")
    return csv_data

def main():
    print('running main function')

    # define root directory
    root_dir = os.getcwd()

    # generate path to config.json files
    config_file_path = os.path.join(root_dir, 'config', 'csv_config.json')
    response_file_path = os.path.join(root_dir, 'config', 'response.json')

    # instantiate required classes 
    print('instantiating classes')
    # api_interactor = API_Interface(response_file_path, csv_file_path)
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
    csv_data, csv_config, csv_file_path = load_configs()
    print(f'configs loaded: \n {csv_data}')
    
    validated_data = validate_data(csv_data, csv_config)

    print('Validation Complete')
    print(f'Validated Data: \n {validated_data}')

if __name__ == "__main__":
    main()
