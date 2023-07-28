import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

class SnowflakeConnector:
    def __init__(self, user, password, account, role):
        self.user = user
        self.password = password
        self.account = account
        self.role = role
        self.conn = None

    def connect(self):
        print("Attempting to connect to Snowflake...")
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            role=self.role
        )
        print("Connected to Snowflake.")

    def close(self):
        print("Attempting to close the connection to Snowflake...")
        self.conn.close()
        print("Closed the connection to Snowflake.")

    def create_and_use_warehouse(self, warehouse_name):
        print(f"Attempting to create warehouse '{warehouse_name}'...")
        self.conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
        print(f"Warehouse '{warehouse_name}' setup complete.")
        print(f"Attempting to use warehouse '{warehouse_name}'...")
        self.conn.cursor().execute(f"USE WAREHOUSE {warehouse_name}")
        print(f"Using Warehouse '{warehouse_name}'.")

    def create_and_use_database(self, database_name):
        print(f"Attempting to create database '{database_name}'...")
        self.conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' setup complete.")
        print(f"Attempting to use database '{database_name}'...")
        self.conn.cursor().execute(f"USE DATABASE {database_name}")
        print(f"Using Database '{database_name}'.")

    def create_and_use_schema(self, schema_name):
        print(f"Attempting to create schema '{schema_name}'...")
        self.conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' setup complete.")
        print(f"Attempting to use schema '{schema_name}'...")
        self.conn.cursor().execute(f"USE SCHEMA {schema_name}")
        print(f"Using Schema '{schema_name}'.")

    def create_table(self, table_name, table_definition):
        print(f"Attempting to create table '{table_name}'...")
        self.conn.cursor().execute(f'CREATE TABLE IF NOT EXISTS "{table_name}"({table_definition})')
        print(f"Table '{table_name}' has been created successfully.")

    def insert_into_table(self, table_name, df):
        print(f"Attempting to insert data into table '{table_name}'...")
        success, num_chunks, num_rows, output = write_pandas(self.conn, df, table_name)
        if not success:
            print("Failed to insert into table")
            raise Exception(f"Failed to insert data into {table_name}")
        print(f"Data has been inserted into Table '{table_name}' successfully. Number of chunks = {num_chunks}, Number of rows = {num_rows}, output = {output}")

    def delete_from_table(self, table_name):
        print(f"Attempting to delete data from table '{table_name}'...")
        self.conn.cursor().execute(f"DELETE FROM {table_name}")
        print(f"All data inside table '{table_name}' has been deleted.")

    def refresh_table(self, table_name, table_definition, df, schema='PUBLIC'):
        print(f"Checking if {table_name} exists...")
        result = self.conn.cursor().execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'")
        result_set = result.fetchall()
        table_exists = len(result_set) > 0
        print(f"Does the table exist? {'Yes' if table_exists else 'No'}")
        if table_exists:
            print(f"Table '{table_name}' exists.")
            result = self.conn.cursor().execute(f"SELECT * FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{schema}' ORDER BY ordinal_position")
            actual_columns = [row[3] for row in result.fetchall()]
            expected_columns = [col.strip().split(' ')[0] for col in table_definition.split(",")]
            if actual_columns != expected_columns:
                print(f"Table '{table_name}' does not match the expected schema.")
                print("Actual schema:", actual_columns)
                print("Expected schema:", expected_columns)
                return
            print("About to call delete_from_table...")
            self.delete_from_table(table_name.upper())
            print("Returned from delete_from_table...")
        else:
            self.create_table(table_name, table_definition)
        self.insert_into_table(table_name, df)
