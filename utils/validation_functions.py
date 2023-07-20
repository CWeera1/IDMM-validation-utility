import json
import re
import datetime

def is_string(value):
    return isinstance(value, str)

def is_integer(value):
    return isinstance(value, int)

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
    # Assuming a custom implementation to check currency data type
    # You can define your own logic here based on specific requirements
    # This is just a placeholder example
    return value.startswith("$")

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