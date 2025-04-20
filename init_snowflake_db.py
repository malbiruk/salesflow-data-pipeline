import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from logger import get_logger

logger = get_logger()

load_dotenv()


def main():
    database = os.getenv("SNOWFLAKE_DATABASE")

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=database,
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    with Path("db_schema/schema.sql").open() as sql_file:
        sql_script = sql_file.read()

    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cursor.execute(f"USE DATABASE {database}")

    statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
    for stmt in statements:
        logger.info("Running:\n%s\n---", stmt)
        try:
            cursor.execute(stmt)
        except snowflake.connector.errors.ProgrammingError as e:
            logger.error("Error executing SQL statement: %s", e)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
