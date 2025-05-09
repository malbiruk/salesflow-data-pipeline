import os

import pandas as pd
from dotenv import load_dotenv
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

load_dotenv()


engine = create_engine(
    URL(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    ),
)


def load_snowflake_data(query: str, **kwargs: object) -> pd.DataFrame:
    """Load data from Snowflake using the provided query."""
    return pd.read_sql(query, engine, **kwargs)
