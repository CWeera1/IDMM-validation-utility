# Data Migration Utility

Data Migration Utility is a Python-based tool for automating the process of uploading CSV files into a Snowflake database. It validates the CSV files, maps the data according to the configuration, and uploads it to the specified tables in Snowflake.

## Features

- Take input as a JSON object detailing the features of a CSV file to be validated
- Validate CSV file format.
- Map CSV data to the corresponding database table structure.
- Load data into a Snowflake database.
- Configurable through a JSON configuration file.
- Easy to extend for supporting additional databases in the future.

## Prerequisites

- Python 3.8 or higher
- Snowflake account

## Configuration

Create a JSON configuration file named `config.json` inside the `config` directory with the following structure:

```json
{
 "source_file_location": "path/to/your/csv/files",
 "file_format": "csv",
 "mapping": {
     "source_column1": "target_column1",
     "source_column2": "target_column2"
 },
 "target_environment": "snowflake",
 "connection_details": {
     "user": "your_snowflake_user",
     "password": "your_snowflake_password",
     "account": "your_snowflake_account",
     "warehouse": "your_snowflake_warehouse",
     "database": "your_snowflake_database",
     "schema": "your_snowflake_schema"
 }
}
```

## Usage

Run the utility with the following command:

```python
python main.py
```


## Directory Overview

### Configuration Management (config/): 
Store your configuration files for different environments and databases. This may include connection strings, credentials, file paths, etc.

### Database Connectors (connectors/):
 This directory should contain separate Python modules for each database you want to connect to. Initially, you can start with snowflake.py, but later you can add other files like postgresql.py, mysql.py, etc. Each module should contain a class or functions that handle connections and data transfers to and from the respective database.

### Validators (validators/): 
This directory should contain modules that validate input files. The csv_validator.py can contain functions or classes that check if the input CSVs are in the expected format, and possibly perform some cleaning or normalization.

### Mappers (mappers/): 
The mappers directory should contain modules for mapping data from the source files to the target data structures. For example, csv_mapper.py could contain logic for how to map columns in the CSV to columns in the target database table.

### Utilities (utils/): 
This directory can contain additional helper functions and classes that are used across the application.

### Tests: 
This directory should contain all your test cases. Following test-driven development (TDD) is a good practice.

### main.py: 
This is the entry point of your application. It should parse input parameters (perhaps from a JSON file), invoke the validators, establish a connection to the appropriate database using the connectors, and perform the data migration using the mappers.

### requirements.txt: 
This file should list all the Python libraries your project depends on.

### Test Log:
- note that the "csv" module automatically handles line breaks (CRLF vs \n) - it is unnecessary to account for this manually, because if the csv file is read in, it implies that the module has handled this behaviour correctly. 

## Module Structure

### CSV Validator Module
Responsible for validating input CSV files.
Functions or classes check CSV format, perform data cleaning, normalization, and handle application-specific validation rules.

### Data Mapper Module
Maps data from source CSV files to target database table structure.
Contains logic defining how CSV columns map to database table columns.

### Database Connector Module
Initially focuses on connecting to Snowflake database.
Functions or classes handle connection establishment, SQL statement execution, and data transfer.

### Configuration Module
Handles loading and parsing JSON configuration file (config/config.json).
Provides functions or classes to access configuration parameters like source file location, file format, mapping, and connection details.

### Utility Module
Contains additional helper functions and classes used across the application.
Includes generic utilities for file handling, logging, and data transformations.

### Main Application Module
Entry point of the application.
Parses input parameters, possibly from a JSON file, and orchestrates modules for data migration.
Invokes CSV validator module, database connector module, and data mapper module based on configuration.
Handles overall workflow.

### Testing Module
Contains test cases for different components of the application.
Follows Test-Driven Development (TDD) principles.
Writes tests to cover functionality of validators, mappers, connectors, and other modules.

### Planned Module Order

1. Config
2. CSV Validation
3. Database Connector
4. Data Mapper
5. Utility Module
6. Main Module
7. Testing Module (will be developed in parallel)

