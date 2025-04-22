"""Configuration management for the ADF pipeline."""

import os

from dotenv import load_dotenv

load_dotenv()


def get_required_env(name: str) -> str:
    """Get environment variable or raise an exception if not found."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_azure_details() -> dict[str, str]:
    """Get Azure resource details."""
    return {
        "tenant_id": get_required_env("AZURE_TENANT"),
        "client_id": get_required_env("AZURE_APP_ID"),
        "client_secret": get_required_env("AZURE_PASSWORD"),
        "subscription_id": get_required_env("AZURE_SUBSCRIPTION_ID"),
        "resource_group": get_required_env("AZURE_RESOURCE_GROUP"),
        "data_factory_name": get_required_env("AZURE_DATAFACTORY_NAME"),
        "storage_account": get_required_env("AZURE_STORAGE_ACCOUNT"),
        "container_name": get_required_env("AZURE_CONTAINER_NAME"),
        "blob_name": get_required_env("AZURE_BLOB_NAME"),
    }


def get_snowflake_details() -> dict[str, str]:
    """Get Snowflake details."""
    return {
        "account": get_required_env("SNOWFLAKE_ACCOUNT"),
        "user": get_required_env("SNOWFLAKE_USER"),
        "password": get_required_env("SNOWFLAKE_PASSWORD"),
        "database": get_required_env("SNOWFLAKE_DATABASE"),
        "warehouse": get_required_env("SNOWFLAKE_WAREHOUSE"),
    }
