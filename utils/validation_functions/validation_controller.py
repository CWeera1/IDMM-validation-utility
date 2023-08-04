import json
import re
import datetime
import csv
from utils.validation_functions.data_type_validators import *
from utils.validation_functions.column_validators import *
from utils.validation_functions.csv_validators import *

class validation_controller:
    def __init__(self, config):
        self.config = config

    def validate_csv(self, input_df):
        """Validate a CSV dataframe against a config."""

        # Copy and lowercase dataframe column headers
        df = input_df.copy()
        df.columns = input_df.columns.str.upper()
        actual_columns = set(df.columns)

        result = True

        # Validate columns
        if not validate_columns(actual_columns, self.config['expected_columns']):
            result = False

        # Check for null values
        if not check_null_values(df):
            result = False

        # Check for datatype consistency
        if not check_datatype_consistency(df, self.config['expected_columns']):
            result = False

        # Validate row count
        if not validate_row_count(df, self.config['expected_rows']):
            result = False

        # Check for NVARCHAR length consistency
        if not check_nvarchar_length(df, self.config['expected_columns']):
            result = False 
        
        # Check for NVARCHAR format consistency
        if not check_nvarchar_format(df, self.config['expected_columns']):
            result = False

        return result # Return True if all checks pass, False otherwise
