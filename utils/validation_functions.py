import json
import re
import datetime
import csv

class CSVValidator:
    def __init__(self, config):
        self.config = config

    def is_nvarchar(self, value):
        return isinstance(value, str)

    def is_bigint(self, value):
        return isinstance(value, int)

    def is_datetime(self, value):
        date_format = "%Y-%m-%dT%H:%M:%S"
        try:
            datetime.datetime.strptime(value, date_format)
            return True
        except (ValueError, TypeError) as e:
            return False

    def is_int(self, value):
        return isinstance(value, int) and -2**31 <= value <= 2**31

    def is_decimal(self, value):
        return isinstance(value, float) or (isinstance(value, str) and '.' in value)

    def is_bit(self, value):
        return value == 1 or 0

    def get_column_set_difference(self, expected, actual):
        """Compute the differences between expected and actual column sets."""
        missing_columns = expected - actual
        unexpected_columns = actual - expected
        return missing_columns, unexpected_columns

    def display_column_differences(self, missing_columns, unexpected_columns):
        """Display column differences."""
        if missing_columns or unexpected_columns:
            print("Mismatched column names detected.")
            if missing_columns:
                print(f"Expected but missing: {list(missing_columns)}")
            if unexpected_columns:
                print(f"Found but not expected: {list(unexpected_columns)}")

    def check_null_values(self, df):
        """Check if the dataframe contains any null values."""
        null_counts = df.isnull().sum()
        if null_counts.any():
            print("The CSV file contains null values in the following columns:")
            print(null_counts[null_counts > 0])

    def check_datatype_consistency(self, df):
        """Check if datatypes in the dataframe are consistent with the expected datatypes."""
        mismatches = {}
        for col in self.config['expected_columns']:
            col_name = col['COLUMN_NAME']
            if col_name not in df.columns: # Check if the column is present in the DataFrame
                continue # Skip to the next column if it's missing

            expected_datatype = col['DATA_TYPE']
            mismatched_entries = df[~df[col_name].map(getattr(self, f'is_{expected_datatype}'))][col_name]
            if not mismatched_entries.empty:
                mismatches[col_name] = mismatched_entries
        if mismatches:
            print("The following columns contain values with unexpected datatypes or incorrect formatting:")
            for col_name, entries in mismatches.items():
                print(f"Column '{col_name}':")
                print(entries)
                print()
            return False

    def validate_row_count(self, df, expected_rows):
        """Validate if the row count of the dataframe matches the expected row count."""
        actual_rows = df.shape[0]
        if expected_rows != actual_rows:
            print(f"The number of rows in the CSV does not match the expected number. Expected: {expected_rows}, but got: {actual_rows}")
            return False
        return True
    
    def check_nvarchar_length(self, df):
        all_columns_valid = True # Tracks if nvarchar columns exceed length
        
        for col in self.config['expected_columns']:
            col_name = col['COLUMN_NAME']
            expected_datatype = col['DATA_TYPE']
            if col_name not in df.columns: # Check if the column is present in the DataFrame
                continue # Skip to the next column if it's missing
            if expected_datatype == 'nvarchar' and 'CHARACTER_MAXIMUM_LENGTH' in col:
                max_length = int(col['CHARACTER_MAXIMUM_LENGTH'])
                too_long_entries = df[df[col_name].apply(lambda x: len(str(x)) > max_length)][col_name]
                if not too_long_entries.empty:
                    print(f"The following entries in column '{col_name}' exceed the maximum length of {max_length}:")
                    print(too_long_entries)
                    print()
                    all_columns_valid = False
        
        return all_columns_valid
    
    def check_nvarchar_format(self, df):
        all_columns_valid = True # Variable to keep track if all nvarchar columns are valid
        
        for col in self.config['expected_columns']:
            col_name = col['COLUMN_NAME']
            expected_datatype = col['DATA_TYPE']
            if col_name not in df.columns: # Check if the column is present in the DataFrame
                continue # Skip to the next column if it's missing

            if expected_datatype == 'nvarchar':
                invalid_format_entries = df[~df[col_name].apply(lambda x: str(x).startswith("N'") and str(x).endswith("'"))][col_name]
                if not invalid_format_entries.empty:
                    print(f"The following entries in column '{col_name}' do not have the correct N'XXX' format:")
                    print(invalid_format_entries)
                    print()
                    all_columns_valid = False
        return all_columns_valid

    def validate_columns(self, actual_columns):
        """Validate that the actual columns match the expected columns."""
        expected_columns = set([col['COLUMN_NAME'] for col in self.config['expected_columns']])
        missing_columns, unexpected_columns = self.get_column_set_difference(expected_columns, actual_columns)
        self.display_column_differences(missing_columns, unexpected_columns)
        return not (missing_columns or unexpected_columns)

    def validate_csv(self, input_df):
        """Validate a CSV dataframe against a config."""

        # Copy and lowercase dataframe column headers
        df = input_df.copy()
        df.columns = input_df.columns.str.upper()
        actual_columns = set(df.columns)

        result = True

        # Validate columns
        if not self.validate_columns(actual_columns):
            result = False

        # Check for null values
        if not self.check_null_values(df):
            result = False

        # Check for datatype consistency
        if not self.check_datatype_consistency(df):
            result = False

        # Validate row count
        if not self.validate_row_count(df, self.config['expected_rows']):
            result = False

        # Check for NVARCHAR length consistency
        if not self.check_nvarchar_length(df):
            result = False 
        
        # Check for NVARCHAR format consistency
        if not self.check_nvarchar_format(df):
            result = False

        return result # Return True if all checks pass, False otherwise

    def clean_data(self, input_df):
        """Clean a validated CSV dataframe for implementation into Snowflake."""

        # Copy and lowercase dataframe column headers
        df = input_df.copy()
        df.columns = df.columns.str.lower()
        
        # Iterate through columns and clean data
        for col in self.config['expected_columns']:
            col_name = col['COLUMN_NAME']
            expected_datatype = col['DATA_TYPE']
            if expected_datatype == 'currency':
                df[col_name] = df[col_name].replace('[\$,]', '', regex=True).astype(float).map('{:.2f}'.format)
                
        return df
