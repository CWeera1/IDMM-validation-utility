# Importing the requests module
import requests

class ColumnDataTypeAPI:
    def __init__(self, response_file_path):
        self.response_file_path = response_file_path

    def get_column_data_types(self):
        # (Same as before)
        with open(self.response_file_path, 'r') as file:
            response_content = file.read()
        response_data = json.loads(response_content)
        column_info = [{"name": column["COLUMN_NAME"], "data_type": column["DATA_TYPE"]} for column in response_data]
        return column_info

    def get_column_data_types_from_api(self, auth_token, csv_headings):
        # Placeholder API endpoint URL
        api_url = "https://api.example.com/get_column_data_types"

        # Headers with authentication token
        headers = {"Authorization": f"Bearer {auth_token}"}

        # Payload with CSV headings
        payload = {"headings": csv_headings}

        # Sending a POST request to the API
        response = requests.post(api_url, json=payload, headers=headers)

        # Parsing the response JSON (assuming a similar structure to the response.txt file)
        response_data = response.json()
        column_info = [{"name": column["COLUMN_NAME"], "data_type": column["DATA_TYPE"]} for column in response_data]

        return column_info

# Example usage with placeholder values (for demonstration purposes)
# auth_token_example = "your_auth_token_here"
# csv_headings_example = ["ID", "POLICYTRANSACTIONID"]
# api_instance = ColumnDataTypeAPI(response_txt_path)
# api_column_data_types = api_instance.get_column_data_types_from_api(auth_token_example, csv_headings_example)
# api_column_data_types
