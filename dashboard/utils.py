import os

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection() -> snowflake.connector.connection:
    """Create a connection to Snowflake."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def load_snowflake_data(query: str) -> pd.DataFrame:
    """Load data from Snowflake using the provided query."""
    conn = get_snowflake_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()
