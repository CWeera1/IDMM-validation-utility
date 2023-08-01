import pandas as pd
import json

def get_data_types_summary(json_file_path):
    # Read the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Extract the data types
    data_types = [column['DATA_TYPE'] for column in data]

    # Create a DataFrame with the data types
    df = pd.DataFrame(data_types, columns=['DATA_TYPE'])

    # Get the counts of unique data types
    summary_df = df['DATA_TYPE'].value_counts().reset_index()
    summary_df.columns = ['DATA_TYPE', 'COUNT']

    return summary_df

# Example usage
json_file_path = 'config/response_ALL.json'
data_types_summary = get_data_types_summary(json_file_path)
print(data_types_summary)
