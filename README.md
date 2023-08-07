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

## Prerequisites

- Python 3.11 (slightly older could still be fine)
- pip

## Configuration (WIP)

1. Create a `config` directory inside the root directory
2. Copy the `API_config_template.json` and `csv_config_template.json` from the root directory into the config folder you just created
3. Rename the copies as `API_config.json` and `csv_config.json` respectively. 
4. Update your newly renamed config files as per your requirements
5. Copy paste the csv's that you want to test into the `tests` directory, double checking that you've updated the `csv_config.json` file to reflect your change
6. Run the following command in your terminal to install the additional dependencies - `pip install -r requirements.txt`
7. Run `python main.py` once all the setup is complete to test your pdf :)

## Usage (WIP - ideally we can change the choice of csv to a CLI argument)

Run the utility with the following command:

```python
python main.py
```