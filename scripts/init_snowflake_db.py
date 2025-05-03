from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from utils import config
from utils.logger import get_logger

logger = get_logger()

load_dotenv()

SCHEMA_RAW = "RAW"


def execute_sql_statements(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    sql_script: str,
) -> None:
    """Execute SQL statements from a script."""
    statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
    for stmt in statements:
        logger.info("Running:\n%s\n---", stmt)
        try:
            cursor.execute(stmt)
        except snowflake.connector.errors.ProgrammingError:
            logger.exception("Error executing SQL statement: %s", stmt)


def main() -> None:
    snowflake_credentials = config.get_snowflake_details()

    conn = snowflake.connector.connect(**snowflake_credentials)
    cursor = conn.cursor()

    database = snowflake_credentials["database"]
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cursor.execute(f"USE DATABASE {database}")

    # Create the raw schema
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")

    # Set up raw schema with a single table
    cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
    with Path("db_schema/raw_schema.sql").open() as sql_file:
        raw_sql = sql_file.read()
        execute_sql_statements(cursor, raw_sql)

    # Normalized and analytics schema will be created by dbt later

    cursor.close()
    conn.close()
    logger.info("Snowflake database initialization completed successfully")


if __name__ == "__main__":
    main()
