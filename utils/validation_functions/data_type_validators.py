import datetime

def is_nvarchar(value):
    return isinstance(value, str)

def is_bigint(value):
    return isinstance(value, int)

def is_datetime(value):
    date_format = "%Y-%m-%dT%H:%M:%S"
    try:
        datetime.datetime.strptime(value, date_format)
        return True
    except (ValueError, TypeError) as e:
        return False

def is_int(value):
    return isinstance(value, int) and -2**31 <= value <= 2**31

def is_decimal(value):
    return isinstance(value, float) or (isinstance(value, str) and '.' in value)

def is_bit(value):
    return value == 1 or 0
