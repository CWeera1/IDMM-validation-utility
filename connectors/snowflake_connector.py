import os
from utils.classes import DatabaseConfig

# Access the environment variables
db_snowflake = DatabaseConfig()
db_snowflake.account = os.getenv("SNOWFLAKE_ACCOUNT")
db_snowflake.user = os.getenv("SNOWFLAKE_USER")
db_snowflake.password = os.getenv("SNOWFLAKE_PASSWORD")
db_snowflake.role = os.getenv("SNOWFLAKE_ROLE")
db_snowflake.database = os.getenv("SNOWFLAKE_DATABASE")
db_snowflake.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
db_snowflake.schema = os.getenv("SNOWFLAKE_SCHEMA")

