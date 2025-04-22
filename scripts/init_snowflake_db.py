from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from utils import config
from utils.logger import get_logger

logger = get_logger()

load_dotenv()

SCHEMA_RAW = "RAW"
SCHEMA_NORMALIZED = "NORMALIZED"
SCHEMA_ANALYTICS = "ANALYTICS"


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

    # Create all schemas
    for schema_name in [SCHEMA_RAW, SCHEMA_NORMALIZED, SCHEMA_ANALYTICS]:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Set up raw schema with a single table
    cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
    with Path("db_schema/raw_schema.sql").open() as sql_file:
        raw_sql = sql_file.read()
        execute_sql_statements(cursor, raw_sql)

    # Set up normalized schema
    cursor.execute(f"USE SCHEMA {SCHEMA_NORMALIZED}")
    with Path("db_schema/normalized_schema.sql").open() as sql_file:
        normalized_sql = sql_file.read()
        execute_sql_statements(cursor, normalized_sql)

    # Set up analytics schema
    cursor.execute(f"USE SCHEMA {SCHEMA_ANALYTICS}")

    cursor.close()
    conn.close()
    logger.info("Snowflake database initialization completed successfully")


if __name__ == "__main__":
    main()
