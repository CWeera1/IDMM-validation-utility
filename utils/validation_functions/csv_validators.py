from utils.validation_functions.data_type_validators import *
import pandas as pd

def check_null_values(df, expected_columns_config):
    """Check if the dataframe contains any null values based on the config."""
    
    # Convert the expected columns list to a dictionary for easier lookup
    column_details = {col['COLUMN_NAME']: col for col in expected_columns_config}

    error_output = []

    for column in df.columns:
        # Check if the column is in the config and if null values aren't allowed
        if column in column_details and column_details[column]['IS_NULLABLE'] == 'NO':
            missing_values = df[column][df[column].isnull()]
            if not missing_values.empty:
                error_output.append(f"Column '{column}':\n{missing_values}\n")

    # Print the collected error outputs
    if error_output:
        print("\n".join(error_output))
    else:
        print("No null values found in non-nullable columns.")

def check_datatype_consistency(df, expected_columns_config):
    """Check if datatypes in the dataframe are consistent with the expected datatypes."""
    mismatches = {}
    for col in expected_columns_config:
        col_name = col['COLUMN_NAME']
        if col_name not in df.columns: # Check if the column is present in the DataFrame
            continue # Skip to the next column if it's missing

        expected_datatype = col['DATA_TYPE']
        validator_function = globals()[f'is_{expected_datatype}']
        mismatched_entries = df[~df[col_name].map(validator_function)][col_name]
        if not mismatched_entries.empty:
            mismatches[col_name] = mismatched_entries
    if mismatches:
        print("The following columns contain values with unexpected datatypes or incorrect formatting:")
        for col_name, entries in mismatches.items():
            print(f"Column '{col_name}':")
            print(entries)
            print()
        return False
    return True

def validate_row_count(df, expected_rows):
    """Validate if the row count of the dataframe matches the expected row count."""
    actual_rows = df.shape[0]
    if expected_rows != actual_rows:
        print(f"The number of rows in the CSV does not match the expected number. Expected: {expected_rows}, but got: {actual_rows}")
        return False
    return True

def check_nvarchar_length(df, expected_columns_config):
    all_columns_valid = True # Tracks if nvarchar columns exceed length
    
    for col in expected_columns_config:
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
    
def check_nvarchar_format(df, expected_columns_config):
    all_columns_valid = True # Variable to keep track if all nvarchar columns are valid
    
    for col in expected_columns_config:
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
