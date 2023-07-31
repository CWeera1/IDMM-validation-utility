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

def is_currency(value):
    """
    Checks if the given value is a string that represents a currency amount.
    The check is based on a regular expression pattern.

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the currency pattern, False otherwise.
    """
    
    # Regular expression for a currency string:
    # - '^'        : Start of the line
    # - '\$'       : Dollar sign
    # - '\d{1,3}'  : Between 1 and 3 digits
    # - '(,\d{3})*': Zero or more groups of comma followed by exactly 3 digits
    # - '(\.\d{2})?': Optional group of dot followed by exactly 2 digits
    # - '$'        : End of the line
    currency_regex = r'^\$\d{1,3}(,\d{3})*(\.\d{2})?$'
    
    # Check if the value matches the currency pattern
    return re.match(currency_regex, value) is not None


def is_url(value):
    """
    Checks if the given value is a string that represents a URL.
    The check is based on a regular expression pattern.

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the URL pattern, False otherwise.
    """
    
    # Regular expression for a URL string:
    # - '^https?' : String starts with 'http' or 'https'
    # - '://'     : Followed by '://'
    # - '(?:[a-zA-Z0-9]|[._-])+' : One or more alphanumeric characters or '.', '_' or '-'
    # - '$'       : End of the line
    url_regex = r'^https?://(?:[a-zA-Z0-9]|[._-])+$'
    
    # Check if the value matches the URL pattern
    return re.match(url_regex, value) is not None


def is_email(value):
    """
    Checks if the given value is a string that represents an email address.
    The check is based on a regular expression pattern.

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the email pattern, False otherwise.
    """
    
    # Regular expression for an email string:
    # - '^'        : Start of the line
    # - '[a-zA-Z0-9._%+-]+' : One or more alphanumeric characters or '.', '_', '%', '+' or '-'
    # - '@'        : At sign '@'
    # - '[a-zA-Z0-9.-]+' : One or more alphanumeric characters or '.' or '-'
    # - '\.[a-zA-Z]{2,}$' : A dot '.' followed by 2 or more alphabetic characters, and end of line
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Check if the value matches the email pattern
    return re.match(email_regex, value) is not None

def is_ip_address(value):
    """
    Checks if the given value is a string that represents an IP address.
    The check is based on a regular expression pattern.

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the IP address pattern, False otherwise.
    """
    
    # Regular expression for an IP address string:
    # - '^'        : Start of the line
    # - '\d{1,3}'  : Between 1 and 3 digits
    # - '\.'       : Dot '.'
    # - '\d{1,3}'  : Between 1 and 3 digits
    # - '\.'       : Dot '.'
    # - '\d{1,3}'  : Between 1 and 3 digits
    # - '\.'       : Dot '.'
    # - '\d{1,3}'  : Between 1 and 3 digits
    # - '$'        : End of the line
    ip_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
    
    # Check if the value matches the IP address pattern
    return re.match(ip_regex, value) is not None

def is_coordinates(value):
    """
    Checks if the given value is a string that represents a coordinate pair.
    The check is based on a regular expression pattern.

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the coordinates pattern, False otherwise.
    """
    
    # Regular expression for a coordinates string:
    # - '^'        : Start of the line
    # - '-?\d+(\.\d+)?' : A digit sequence, optionally preceded by '-' and optionally followed by '.' and more digits
    # - ','        : Comma ','
    # - '-?\d+(\.\d+)?' : A digit sequence, optionally preceded by '-' and optionally followed by '.' and more digits
    # - '$'        : End of the line
    coordinates_regex = r'^-?\d+(\.\d+)?,-?\d+(\.\d+)?$'
    
    # Check if the value matches the coordinates pattern
    return re.match(coordinates_regex, value) is not None

def is_binary_data(value):
    """
    Checks if the given value is a string that represents binary data (sequence of 0s and 1s).
    The check is based on a regular expression pattern.

    Parameters:
    value (str): The value to be checked.

    Returns:
    bool: True if the value matches the binary data pattern, False otherwise.
    """
    
    # Regular expression for a binary data string:
    # - '^'        : Start of the line
    # - '[01]+'    : One or more of '0' or '1'
    # - '$'        : End of the line
    binary_regex = r'^[01]+$'
    
    # Check if the value matches the binary data pattern
    return re.match(binary_regex, value) is not None

def get_column_set_difference(expected, actual):
    """Compute the differences between expected and actual column sets."""
    missing_columns = expected - actual
    unexpected_columns = actual - expected
    return missing_columns, unexpected_columns

def display_column_differences(missing_columns, unexpected_columns):
    """Display column differences."""
    if missing_columns or unexpected_columns:
        print("Mismatched column names detected.")
        if missing_columns:
            print(f"Expected but missing: {list(missing_columns)}")
        if unexpected_columns:
            print(f"Found but not expected: {list(unexpected_columns)}")

def check_null_values(df):
    """Check if the dataframe contains any null values."""
    null_counts = df.isnull().sum()
    if null_counts.any():
        print("The CSV file contains null values in the following columns:")
        print(null_counts[null_counts > 0])

def check_datatype_consistency(df, config):
    """Check if datatypes in the dataframe are consistent with the expected datatypes."""
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

def validate_row_count(df, expected_rows):
    """Validate if the row count of the dataframe matches the expected row count."""
    actual_rows = df.shape[0]
    if expected_rows != actual_rows:
        print(f"The number of rows in the CSV does not match the expected number. Expected: {expected_rows}, but got: {actual_rows}")
        return False
    return True

def validate_csv(input_df, config):
    """Validate a CSV dataframe against a config."""

    # Copy and lowercase dataframe column headers
    df = input_df.copy()
    df.columns = input_df.columns.str.lower()

    # Check for column mismatches
    expected_columns = set([col['name'] for col in config['expected_columns']])
    actual_columns = set(df.columns)
    missing_columns, unexpected_columns = get_column_set_difference(expected_columns, actual_columns)
    display_column_differences(missing_columns, unexpected_columns)

    # Check for null values
    check_null_values(df)

    # Check for datatype consistency
    check_datatype_consistency(df, config)

    # Validate row count
    if not validate_row_count(df, config['expected_rows']):
        return False

    return True  # Return True if all checks pass, False otherwise

def clean_data(input_df, config):
    """Clean a validated CSV dataframe for implementation into Snowflake."""

    # Copy and lowercase dataframe column headers
    df = input_df.copy()
    df.columns = df.columns.str.lower()
    
    # Iterate through columns and clean data
    for col in config['expected_columns']:
        col_name = col['name']
        expected_datatype = col['datatype']
        if expected_datatype == 'currency':
            df[col_name] = df[col_name].replace('[\$,]', '', regex=True).astype(float).map('{:.2f}'.format)
            
    return df