"""A module for providing connection and interaction with Snowflake databases.

This module includes functions to establish a connection with Snowflake, set up the environment
(warehouse, database, schema), interact with the database (create, alter, drop tables, insert, select, update, delete data), 
and close the connection. Environment variables are used to store Snowflake credentials.
"""

import os
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from utils.classes import DatabaseConfig


# ------------------------ Setup ------------------------

def get_snowflake_config():
    """Retrieve Snowflake configuration from environment variables."""
    # Access the environment variables
    db_snowflake = DatabaseConfig()
    db_snowflake.account = os.getenv("SNOWFLAKE_ACCOUNT")
    db_snowflake.user = os.getenv("SNOWFLAKE_USER")
    db_snowflake.password = os.getenv("SNOWFLAKE_PASSWORD")
    db_snowflake.role = os.getenv("SNOWFLAKE_ROLE")
    db_snowflake.database = os.getenv("SNOWFLAKE_DATABASE")
    db_snowflake.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    db_snowflake.schema = os.getenv("SNOWFLAKE_SCHEMA")
    
    return db_snowflake


def connect_to_snowflake(db_snowflake):
    """Establish a connection to Snowflake."""
    try:
        # Establish the Snowflake connection
        conn = snowflake.connector.connect(
            user=db_snowflake.user,
            password=db_snowflake.password,
            account=db_snowflake.account
        )
        print("Connected to Snowflake.")
        return conn
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while connecting to Snowflake: {error_code}")


def create_warehouse(conn, warehouse_name):
    """Create a warehouse if it doesn't exist."""
    try:
        conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
        print(f"Warehouse '{warehouse_name}' setup complete.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating the warehouse '{warehouse_name}': {error_code}")


def use_warehouse(conn, warehouse_name):
    """Switch to a specific warehouse."""
    try:
        conn.cursor().execute(f"USE WAREHOUSE {warehouse_name}")
        print(f"Using Warehouse '{warehouse_name}'.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while using the warehouse '{warehouse_name}': {error_code}")


def create_database(conn, database_name):
    """Create a database if it doesn't exist."""
    try:
        conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' setup complete.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating the database '{database_name}': {error_code}")


def use_database(conn, database_name):
    """Switch to a specific database."""
    try:
        conn.cursor().execute(f"USE DATABASE {database_name}")
        print(f"Using Database '{database_name}'.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while using the database '{database_name}': {error_code}")


def create_schema(conn, schema_name):
    """Create a schema if it doesn't exist."""
    try:
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' setup complete.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating the schema '{schema_name}': {error_code}")


def use_schema(conn, database_name, schema_name):
    """Switch to a specific schema within a database."""
    try:
        conn.cursor().execute(f"USE SCHEMA {database_name}.{schema_name}")
        print(f"Using Schema '{schema_name}' in Database '{database_name}'.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while using the schema '{schema_name}' in database '{database_name}': {error_code}")


def setup_snowflake(conn, warehouse_name, database_name, schema_name):
    """Perform setup for Snowflake by creating and using warehouse, database, and schema."""
    create_warehouse(conn, warehouse_name)
    use_warehouse(conn, warehouse_name)
    create_database(conn, database_name)
    use_database(conn, database_name)
    create_schema(conn, schema_name)
    use_schema(conn, database_name, schema_name)

# ------------------------ Interaction ------------------------


def create_table(conn, table_name, schema):
    """Create a table in the Snowflake database."""
    try:
        # Creating a table
        conn.cursor().execute(f"CREATE TABLE {table_name}({schema})")
        print(f"Table '{table_name}' has been created successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while creating table '{table_name}': {error_code}")


def drop_table(conn, table_name):
    """Drop a table in the Snowflake database."""
    try:
        # Dropping a table
        conn.cursor().execute(f"DROP TABLE {table_name}")
        print(f"Table '{table_name}' has been dropped successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while dropping table '{table_name}': {error_code}")


def alter_table(conn, table_name, operation):
    """Alter a table in the Snowflake database."""
    try:
        # Altering a table
        conn.cursor().execute(f"ALTER TABLE {table_name} {operation}")
        print(f"Table '{table_name}' has been altered successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while altering table '{table_name}': {error_code}")


def insert_into_table(conn, table_name, df):
    """Insert a dataframe into a table in the Snowflake database."""
    try:
        # inserting dataframe data into the table
        write_pandas(conn, df, table_name)
        print(f"Data has been inserted into Table '{table_name}' successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while inserting data into table '{table_name}': {error_code}")


def select_from_table(conn, table_name, columns='*'):
    """Select data from a table in the Snowflake database."""
    try:
        # selecting data from table
        cursor = conn.cursor()
        cursor.execute(f"SELECT {columns} FROM {table_name}")
        data = cursor.fetchall()
        print(f"Data has been selected from Table '{table_name}' successfully.")
        return data
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while selecting data from table '{table_name}': {error_code}")


def update_table(conn, table_name, set_conditions, where_condition):
    """Update records in a table in the Snowflake database."""
    try:
        # updating table
        conn.cursor().execute(f"UPDATE {table_name} SET {set_conditions} WHERE {where_condition}")
        print(f"Table '{table_name}' has been updated successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while updating table '{table_name}': {error_code}")


def delete_from_table(conn, table_name, where_condition):
    """Delete records from a table in the Snowflake database."""
    try:
        # deleting from table
        conn.cursor().execute(f"DELETE FROM {table_name} WHERE {where_condition}")
        print(f"Records have been deleted from Table '{table_name}' successfully.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while deleting records from table '{table_name}': {error_code}")


def close_connection(conn):
    """Close the connection to the Snowflake database."""
    try:
        conn.close()
        print("Closed the connection to Snowflake.")
    except snowflake.connector.Error as error_code:
        print(f"An error occurred while closing the connection: {error_code}")
