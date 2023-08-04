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

def validate_columns(actual_columns, expected_columns_config):
    """Validate that the actual columns match the expected columns."""
    expected_columns = set([col['COLUMN_NAME'] for col in expected_columns_config])
    missing_columns, unexpected_columns = get_column_set_difference(expected_columns, actual_columns)
    display_column_differences(missing_columns, unexpected_columns)
    return not (missing_columns or unexpected_columns)
