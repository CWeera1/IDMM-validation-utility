import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

class SnowflakeConnector:
    def __init__(self, database_details):
        # Initializing the SnowflakeConnector with database details
        self.user = database_details['user']
        self.password = database_details['password']
        self.account = database_details['account']
        self.role = database_details['role']
        self.database = database_details['database']
        self.warehouse = database_details['warehouse']
        self.schema = database_details['schema']
        
        self.conn = None  # Placeholder for the connection object

    def connect(self):
        # Connecting to the Snowflake database
        print("Attempting to connect to Snowflake...")
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            role=self.role
        )
        print("Connected to Snowflake.")

    def close(self):
        # Closing the connection to the Snowflake database
        print("Attempting to close the connection to Snowflake...")
        self.conn.close()
        print("Closed the connection to Snowflake.")

    def create_and_use_warehouse(self, warehouse_name):
        # Creating and setting the warehouse for use
        print(f"Attempting to create warehouse '{warehouse_name}'...")
        self.conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
        print(f"Warehouse '{warehouse_name}' setup complete.")
        print(f"Attempting to use warehouse '{warehouse_name}'...")
        self.conn.cursor().execute(f"USE WAREHOUSE {warehouse_name}")
        print(f"Using Warehouse '{warehouse_name}'.")

    def create_and_use_database(self, database_name):
        # Creating and setting the database for use
        print(f"Attempting to create database '{database_name}'...")
        self.conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' setup complete.")
        print(f"Attempting to use database '{database_name}'...")
        self.conn.cursor().execute(f"USE DATABASE {database_name}")
        print(f"Using Database '{database_name}'.")

    def create_and_use_schema(self, schema_name):
        # Creating and setting the schema for use
        print(f"Attempting to create schema '{schema_name}'...")
        self.conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' setup complete.")
        print(f"Attempting to use schema '{schema_name}'...")
        self.conn.cursor().execute(f"USE SCHEMA {schema_name}")
        print(f"Using Schema '{schema_name}'.")

    def create_table(self, table_name, table_definition):
        # Creating a table with a given definition
        print(f"Attempting to create table '{table_name}'...")
        self.conn.cursor().execute(f'CREATE TABLE IF NOT EXISTS "{table_name}"({table_definition})')
        print(f"Table '{table_name}' has been created successfully.")


    def create_table_definition(self, config):
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
            'binary_data': 'BINARY',
        }
        
        # Initialize an empty list to hold column definitions
        columns = []

        # For each expected column in the config, create a column definition
        for column in config['expected_columns']:
            
            # Change to uppercase and load columns into SQL
            column['COLUMN_NAME'] = column['COLUMN_NAME'].upper()
            print(f"Processing column '{column['COLUMN_NAME']}' with datatype '{column['DATA_TYPE']}'...")
            
            # Map the CSV config data type to a Snowflake SQL data type
            sql_type = type_mapping.get(column['DATA_TYPE'], 'TEXT')
            print(f"Mapped CSV config datatype '{column['DATA_TYPE']}' to SQL data type '{sql_type}'.")

            # Add the column definition to the list
            columns.append(f"{column['COLUMN_NAME']} {sql_type}")
            print(f"Added column definition '{column['COLUMN_NAME']} {sql_type}' to schema.")

        # Join the column definitions into a single string, separated by commas
        table_definition = ', '.join(columns)

        print("Finished generating schema.")
        
        return table_definition

    def setup_snowflake(self, warehouse_name, database_name, schema_name):
        """Perform setup for Snowflake by creating and using warehouse, database, and schema."""
        print("Starting Snowflake setup...")
        
        # Create and use warehouse
        self.create_and_use_warehouse(warehouse_name)
        
        # Create and use database
        self.create_and_use_database(database_name)
        
        # Create and use schema
        self.create_and_use_schema(schema_name)

        print("Snowflake setup complete.")

    def insert_into_table(self, table_name, df):
        # Inserting data from a DataFrame into a table
        print(f"Attempting to insert data into table '{table_name}'...")
        success, num_chunks, num_rows, output = write_pandas(self.conn, df, table_name)
        if not success:
            print("Failed to insert into table")
            raise Exception(f"Failed to insert data into {table_name}")
        print(f"Data has been inserted into Table '{table_name}' successfully. Number of chunks = {num_chunks}, Number of rows = {num_rows}, output = {output}")

    def delete_from_table(self, table_name):
        # Deleting all data from a given table
        print(f"Attempting to delete data from table '{table_name}'...")
        self.conn.cursor().execute(f"DELETE FROM {table_name}")
        print(f"All data inside table '{table_name}' has been deleted.")

    def refresh_table(self, table_name, table_definition, df, schema='PUBLIC'):
        # Refreshing a table by checking its existence, comparing schema, and inserting new data
        print(f"Checking if {table_name} exists...")
        result = self.conn.cursor().execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'")
        result_set = result.fetchall()
        table_exists = len(result_set) > 0
        print(f"Does the table exist? {'Yes' if table_exists else 'No'}")
        if table_exists:
            # If table exists, compare actual and expected columns, then delete and insert data
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
            # If table does not exist, create it
            self.create_table(table_name, table_definition)
        # Insert data into the table
        self.insert_into_table(table_name, df)

