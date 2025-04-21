import os

from azure.mgmt.datafactory.models import (
    AzureBlobStorageLinkedService,
    LinkedServiceResource,
    SecureString,
    SnowflakeLinkedService,
)

from salesflow_data_pipeline.adf import config
from salesflow_data_pipeline.utils.logger import get_logger

logger = get_logger()


def create_linked_services() -> tuple[LinkedServiceResource]:
    """Create linked services for source and sink."""
    logger.info("Creating linked services...")

    storage_details = config.get_storage_details()
    snowflake_details = config.get_snowflake_details()

    azure_blob_linked_service = LinkedServiceResource(
        properties=AzureBlobStorageLinkedService(
            connection_string=storage_details["connection_string"],
        ),
    )

    snowflake_linked_service = LinkedServiceResource(
        properties=SnowflakeLinkedService(
            connection_string=f"account={snowflake_details['account']};",
            authentication_type="Basic",
            user_name=os.getenv("SNOWFLAKE_USER"),
            password=SecureString(value=snowflake_details["password"]),
        ),
    )

    return azure_blob_linked_service, snowflake_linked_service
