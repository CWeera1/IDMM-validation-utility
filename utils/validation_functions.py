def validate_csv(df, config):
    # Does the CSV file have headings matching the JSON object?
    expected_columns = [col['name'] for col in config['expected_columns']]
    if set(expected_columns) != set(df.columns):
        print(f"Column names in the CSV do not match the expected names. Expected: {expected_columns}, but got: {list(df.columns)}")
        return False

    # Does the CSV file have nulls? If so, where?
    null_counts = df.isnull().sum()
    if null_counts.any():
        print("The CSV file contains null values in the following columns:")
        print(null_counts[null_counts > 0])
        return False

    # Are the datatypes throughout the CSV consistent with the datatype specified in the JSON object?
    for col in config['expected_columns']:
        col_name = col['name']
        expected_datatype = col['datatype']
        if not df[col_name].map(globals()[f'is_{expected_datatype}']).all():
            print(f"Column '{col_name}' contains values that do not match the expected datatype '{expected_datatype}'.")
            return False

    # Does it have the same rows as expected?
    expected_rows = config['expected_rows']
    actual_rows = df.shape[0]
    if expected_rows != actual_rows:
        print(f"The number of rows in the CSV does not match the expected number. Expected: {expected_rows}, but got: {actual_rows}")
        return False

    return True  # return True if all checks pass, False otherwise

