# Importing the requests module
import csv
import json
import requests

class CSV_Config_Manager:
    def __init__(self, config_file_path, response_file_path):
        self.config_file_path = config_file_path
        self.response_file_path = response_file_path
        self.load_config()
        self.read_response()
        self.update_csv_config()

    def load_config(self):
        # Read in the config file from csv_config.json
        with open(self.config_file_path, 'r') as file:
            self.config = json.load(file)

    def read_response(self):
        # Read the column data types from response.json
        with open(self.response_file_path, 'r') as file:
            self.column_info = json.load(file)

    def update_csv_config(self):
        # Update the expected_columns with the new column_info
        self.config['expected_columns'] = self.column_info

        # Write the updated config back to the file
        with open(self.config_file_path, 'w') as file:
            json.dump(self.config, file, indent=4)


class API_Interface:
    def __init__(self, response_file_path, csv_file_path):
        self.response_file_path = response_file_path
        self.csv_file_path = csv_file_path

    def get_csv_headings(self):
        # Read the CSV file to get the headings
        with open(self.csv_file_path, newline='') as file:
            reader = csv.reader(file)
            headings = next(reader)  # Get the first row, which contains the headings
        return headings

    def get_column_data_types_from_api(self, auth_token):
        # Get the CSV headings from the specified file
        csv_headings = self.get_csv_headings()

        # Placeholder API endpoint URL
        api_url = "https://api.example.com/get_column_data_types"

        # Headers with authentication token
        headers = {"Authorization": f"Bearer {auth_token}"}

        # Payload with CSV headings
        payload = {"headings": csv_headings}

        # Sending a POST request to the API
        response = requests.post(api_url, json=payload, headers=headers)

        # Parsing the response JSON
        response_data = response.json()

        # Writing the response to the response.json file
        with open(self.response_file_path, 'w') as file:
            json.dump(response_data, file, indent=4)

        column_info = [{"name": column["COLUMN_NAME"], "data_type": column["DATA_TYPE"]} for column in response_data]

        return column_info


# Example usage with placeholder values (for demonstration purposes)
# auth_token_example = "your_auth_token_here"
# csv_headings_example = ["ID", "POLICYTRANSACTIONID"]
# api_instance = ColumnDataTypeAPI(response_txt_path)
# api_column_data_types = api_instance.get_column_data_types_from_api(auth_token_example, csv_headings_example)
# api_column_data_types
