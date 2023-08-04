# Data Migration Utility - WIP

Data Migration Utility is a Python-based tool for validating CSV files for upload into Infosistema DMM via accessing an external API. 

## Scope Change

- Now required to take in CSV and access API to look for matching columns and information
- Receives response and validates CSV against API response
- JSON object to provide configuration details and act as temporary source for column headings
- Validates CSV for the following:
    - Does the CSV file have headers matching the JSON object?
    - Does the CSV file have nulls?
    - Are the datatypes consistent?
    - Does it have the expected number of rows?
    - Does it use the correct delimiter?
    - Are any strings too long?
    - Are any datetimes incorrectly formatted?
    - Are any expected columns missing?
    - Are any unexpected columns present?

## Pending Functionality

- Awaiting access to workspace to implement API access functionality

## Configuration (WIP)

Create a JSON configuration file named `csv_config.json` inside the `config` directory with the following structure:

```json

{
 "source_file_name": "datatype_consistency.csv",
    "expected_rows": 10,
    "expected_delimiter": ";",
}

```

Note that this file will be modified with output from the API call. 

Create a JSON configuration file named `API_config.json` inside the `config` directory with the following structure:

```json

{
    "api_url": "https://api.example.com/get_column_data_types",
    "auth_token": "your_auth_token_here"
}


```


## Usage (WIP - ideally we can change the choice of csv to a CLI argument)

Run the utility with the following command:

```python
python main.py
```