"""A module for providing connection and interaction with Snowflake databases.


This module includes functions to establish a connection with Snowflake, set up the environment
(warehouse, database, schema), interact with the database (create, alter, drop tables, insert, select, update, delete data), 
and close the connection. Environment variables are used to store Snowflake credentials.
"""
import pandas
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def connect_to_snowflake(db_snowflake):
    """Establish a connection to Snowflake."""
    print("Attempting to connect to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=db_snowflake["user"],
            password=db_snowflake["password"],
            account=db_snowflake["account"],
            role=db_snowflake["role"]
        )
        print("Connected to Snowflake.")
        return conn
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while connecting to Snowflake: {error_code}")


def create_warehouse(conn, warehouse_name):
    """Create a warehouse if it doesn't exist."""
    print(f"Attempting to create warehouse '{warehouse_name}'...")
    try:
        conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
        print(f"Warehouse '{warehouse_name}' setup complete.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating the warehouse '{warehouse_name}': {error_code}")

def use_warehouse(conn, warehouse_name):
    """Switch to a specific warehouse."""
    print(f"Attempting to use warehouse '{warehouse_name}'...")
    try:
        conn.cursor().execute(f"USE WAREHOUSE {warehouse_name}")
        print(f"Using Warehouse '{warehouse_name}'.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while using the warehouse '{warehouse_name}': {error_code}")

def create_database(conn, database_name):
    """Create a database if it doesn't exist."""
    print(f"Attempting to create database '{database_name}'...")
    try:
        conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' setup complete.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating the database '{database_name}': {error_code}")

def use_database(conn, database_name):
    """Switch to a specific database."""
    print(f"Attempting to use database '{database_name}'...")
    try:
        conn.cursor().execute(f"USE DATABASE {database_name}")
        print(f"Using Database '{database_name}'.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while using the database '{database_name}': {error_code}")

def create_schema(conn, schema_name):
    """Create a schema if it doesn't exist."""
    print(f"Attempting to create schema '{schema_name}'...")
    try:
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' setup complete.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating the schema '{schema_name}': {error_code}")

def use_schema(conn, database_name, schema_name):
    """Switch to a specific schema within a database."""
    print(f"Attempting to use schema '{schema_name}' in database '{database_name}'...")
    try:
        conn.cursor().execute(f"USE SCHEMA {database_name}.{schema_name}")
        print(f"Using Schema '{schema_name}' in Database '{database_name}'.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while using the schema '{schema_name}' in database '{database_name}': {error_code}")


def create_table_definition(config):
    """Create a schema based on the CSV config file."""
    print("Starting to generate schema...")
    
    # Mapping from CSV config data types to Snowflake SQL data types
    type_mapping = {
        'string': 'TEXT',
        'integer': 'INTEGER',
        'positive_integer': 'INTEGER',
        'gender': 'TEXT',
        'float': 'FLOAT',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP',
        'boolean': 'BOOLEAN',
        'percentage': 'FLOAT',
        'scientific_notation': 'FLOAT',
        'enum': 'TEXT',
        'json': 'VARIANT',
        'currency': 'FLOAT',
        'url': 'TEXT',
        'email': 'TEXT',
        'ip_address': 'TEXT',
        'coordinates': 'TEXT',
        'binary_data': 'BINARY'
    }
    
    # Initialize an empty list to hold column definitions
    columns = []

    # For each expected column in the config, create a column definition
    for column in config['expected_columns']:
        
        # Change to uppercase and load columns into SQL
        column['name'] = column['name'].upper()
        print(f"Processing column '{column['name']}' with datatype '{column['datatype']}'...")
        
        # Map the CSV config data type to a Snowflake SQL data type
        sql_type = type_mapping.get(column['datatype'], 'TEXT')
        print(f"Mapped CSV config datatype '{column['datatype']}' to SQL data type '{sql_type}'.")

        # Add the column definition to the list
        columns.append(f"{column['name']} {sql_type}")
        print(f"Added column definition '{column['name']} {sql_type}' to schema.")

    # Join the column definitions into a single string, separated by commas
    table_definition = ', '.join(columns)

    print("Finished generating schema.")
    
    return table_definition

def setup_snowflake(conn, warehouse_name, database_name, schema_name):
    """Perform setup for Snowflake by creating and using warehouse, database, and schema."""
    print("Starting Snowflake setup...")
    create_warehouse(conn, warehouse_name)
    use_warehouse(conn, warehouse_name)
    create_database(conn, database_name)
    use_database(conn, database_name)
    create_schema(conn, schema_name)
    use_schema(conn, database_name, schema_name)
    print("Snowflake setup complete.")

def create_table(conn, table_name, table_definition):
    """Create a table in the Snowflake database."""
    print(f"Attempting to create table '{table_name}'...")
    try:
        conn.cursor().execute(f'CREATE TABLE IF NOT EXISTS "{table_name}"({table_definition})')
        print(f"Table '{table_name}' has been created successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating table '{table_name}': {error_code}")

def insert_into_table(conn, table_name, df):
    """Insert a dataframe into a table in the Snowflake database."""
    print(f"Attempting to insert data into table '{table_name}'...")
    try:
        success, num_chunks, num_rows, output = write_pandas(conn, df, table_name)
        if success:
            print(f"Data has been inserted into Table '{table_name}' successfully. Number of chunks = {num_chunks}, Number of rows = {num_rows}, output = {output}")
        else:
            print("Failed to insert into table")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while inserting data into table '{table_name}': {error_code}")


def delete_from_table(conn, table_name):
    """Delete all rows from a table in the Snowflake database."""
    print(f"Attempting to delete data from table '{table_name}'...")
    try:
        conn.cursor().execute(f"DELETE FROM {table_name}")
        print(f"All data inside table '{table_name}' has been deleted.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while deleting data from table '{table_name}': {error_code}")

def refresh_table(conn, table_name, table_definition, df, schema='PUBLIC'):
    """Refresh a table in the Snowflake database."""
    try:        
        # Check if the table exists
        print(f"Checking if {table_name} exists...")
        result = conn.cursor().execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'")
        result_set = result.fetchall()  # Fetch all rows of the result set
        print("Result set:", result_set)

        table_exists = len(result_set) > 0  # Check if the result set is not empty
        print(f"Does the table exist? {'Yes' if table_exists else 'No'}")

        if table_exists:
            print(f"Table '{table_name}' exists.")
            
            # Check if the table columns matches the expected columns
            result = conn.cursor().execute(f"SELECT * FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{schema}' ORDER BY ordinal_position")
            actual_columns = [row[3] for row in result.fetchall()]  # Change 0 to 1
            expected_columns = [col.strip().split(' ')[0] for col in table_definition.split(",")]

            if actual_columns != expected_columns:
                print(f"Table '{table_name}' does not match the expected schema.")
                print("Actual schema:", actual_columns)
                print("Expected schema:", expected_columns)
                return

            # Use the delete_from_table function to clear the data
            print("About to call delete_from_table...")
            delete_from_table(conn, table_name.upper())
            print("Returned from delete_from_table...")

        else:
            # Use the create_table function to create the table if it doesn't exist
            create_table(conn, table_name, table_definition)

        # Use the insert_into_table function to insert the data into the table
        insert_into_table(conn, table_name, df)

    except snowflake.connector.Error as error_code:
        print(f"An error occurred while refreshing table '{table_name}': {error_code}")


def close_connection(conn):
    """Close the connection to the Snowflake database."""
    print("Attempting to close the connection to Snowflake...")
    try:
        conn.close()
        print("Closed the connection to Snowflake.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while closing the connection: {error_code}")
