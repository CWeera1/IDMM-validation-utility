import json
import re
import datetime

def is_string(value):
    return isinstance(value, str)

def is_integer(value):
    return isinstance(value, int)

def is_positive_integer(value):
    if isinstance(value, int):
        return value > 0
    else:
        return False

def is_gender(value):
    return value in {'m', 'M', 'f', 'F', 'male', 'Male', 'female', 'Female'}

def is_float(value):
    return isinstance(value, float)

def is_date(value):
    return isinstance(value, datetime.date)

def is_time(value):
    return isinstance(value, datetime.time)

def is_datetime(value):
    return isinstance(value, datetime.datetime)

def is_boolean(value):
    return isinstance(value, bool)

def is_currency(value):
    """
    This function checks if the given value is a string that represents a currency amount.
    The check is based on a regular expression pattern that matches a dollar sign followed by 
    a number with optional thousands separators (commas) and optional decimal places 
    (two digits after a period).

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the currency pattern, False otherwise.
    """
    
    # Define the regular expression pattern for a currency string
    currency_regex = r'^\$\d{1,3}(,\d{3})*(\.\d{2})?$'
    
    # Check if the value matches the currency pattern
    return re.match(currency_regex, value) is not None


def is_percentage(value):
    # Assuming a custom implementation to check percentage data type
    # You can define your own logic here based on specific requirements
    # This is just a placeholder example
    return value.endswith("%")

def is_scientific_notation(value):
    # Assuming a custom implementation to check scientific notation data type
    # You can define your own logic here based on specific requirements
    # This is just a placeholder example
    return "E+" in value or "E-" in value

def is_enum(value, enum_values):
    # Assuming a custom implementation to check enumeration data type
    # You can define your own logic here based on specific requirements
    # This is just a placeholder example
    return value in enum_values

def is_json(value):
    try:
        json.loads(value)
        return True
    except ValueError:
        return False

def is_url(value):
    # Assuming a basic URL format check
    # You can use a library like `validators` for more comprehensive URL validation
    # This is just a placeholder example
    url_regex = r'^https?://(?:[a-zA-Z0-9]|[._-])+$'
    return re.match(url_regex, value) is not None

def is_email(value):
    # Assuming a basic email format check
    # You can use a library like `email-validator` for more comprehensive email validation
    # This is just a placeholder example
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, value) is not None

def is_ip_address(value):
    # Assuming a basic IP address format check
    # You can use a library like `ipaddress` for more comprehensive IP address validation
    # This is just a placeholder example
    ip_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
    return re.match(ip_regex, value) is not None

def is_coordinates(value):
    # Assuming a basic coordinates format check
    # You can define your own logic based on specific requirements
    # This is just a placeholder example
    coordinates_regex = r'^-?\d+(\.\d+)?,-?\d+(\.\d+)?$'
    return re.match(coordinates_regex, value) is not None

def is_binary_data(value):
    # Assuming a basic check for binary data
    # You can define your own logic based on specific requirements
    # This is just a placeholder example
    binary_regex = r'^[01]+$'
    return re.match(binary_regex, value) is not None

def validate_csv(df, config):
    # Change column headings to lowercase
    df.columns = df.columns.str.lower()
    
    # Does the CSV file have headings matching the JSON object?
    expected_columns = set([col['name'] for col in config['expected_columns']])
    actual_columns = set(df.columns)
    missing_columns = expected_columns - actual_columns
    unexpected_columns = actual_columns - expected_columns

    if missing_columns or unexpected_columns:
        print("Mismatched column names detected.")
        if missing_columns:
            print(f"Expected but missing: {list(missing_columns)}")
        if unexpected_columns:
            print(f"Found but not expected: {list(unexpected_columns)}")
        return False


    # Does the CSV file have nulls? If so, where?
    null_counts = df.isnull().sum()
    if null_counts.any():
        print("The CSV file contains null values in the following columns:")
        print(null_counts[null_counts > 0])
        return False

    # Are the datatypes throughout the CSV consistent with the datatype specified in the JSON object?
    mismatches = {}
    for col in config['expected_columns']:
        col_name = col['name']
        expected_datatype = col['datatype']
        mismatched_entries = df[~df[col_name].map(globals()[f'is_{expected_datatype}'])][col_name]
        if not mismatched_entries.empty:
            mismatches[col_name] = mismatched_entries
    if mismatches:
        print("The following columns contain values that do not match the expected datatypes:")
        for col_name, entries in mismatches.items():
            print(f"Column '{col_name}':")
            print(entries)
            print()
        return False

    # Does it have the same rows as expected?
    expected_rows = config['expected_rows']
    actual_rows = df.shape[0]
    if expected_rows != actual_rows:
        print(f"The number of rows in the CSV does not match the expected number. Expected: {expected_rows}, but got: {actual_rows}")
        return False

    return True  # return True if all checks pass, False otherwise

