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
        return isinstance(value, datetime.datetime)

    def is_int(self, value):
        return isinstance(value, int)

    def is_decimal(self, value):
        return isinstance(value, float) or (isinstance(value, str) and '.' in value)

    def is_bit(self, value):
        return isinstance(value, bool)

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
            expected_datatype = col['DATA_TYPE']
            mismatched_entries = df[~df[col_name].map(getattr(self, f'is_{expected_datatype}'))][col_name]
            if not mismatched_entries.empty:
                mismatches[col_name] = mismatched_entries
        if mismatches:
            print("The following columns contain values that do not match the expected datatypes:")
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

    def validate_csv(self, input_df):
        """Validate a CSV dataframe against a config."""

        # Copy and lowercase dataframe column headers
        df = input_df.copy()
        df.columns = input_df.columns.str.upper()

        # Check for column mismatches
        expected_columns = set([col['COLUMN_NAME'] for col in self.config['expected_columns']])
        actual_columns = set(df.columns)
        missing_columns, unexpected_columns = self.get_column_set_difference(expected_columns, actual_columns)
        self.display_column_differences(missing_columns, unexpected_columns)

        # Check for null values
        self.check_null_values(df)

        # Check for datatype consistency
        self.check_datatype_consistency(df)

        # Validate row count
        if not self.validate_row_count(df, self.config['expected_rows']):
            return False

        return True  # Return True if all checks pass, False otherwise

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
