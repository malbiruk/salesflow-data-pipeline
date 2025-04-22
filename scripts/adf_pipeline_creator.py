"""Script to load data from Azure Blob Storage to Snowflake RAW schema."""

import time

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityPolicy,
    AzureBlobStorageLinkedService,
    AzureBlobStorageLocation,
    AzureBlobStorageReadSettings,
    CopyActivity,
    CopySink,
    CopySource,
    DatasetReference,
    DatasetResource,
    DelimitedTextDataset,
    DelimitedTextReadSettings,
    Factory,
    LinkedServiceReference,
    LinkedServiceResource,
    PipelineResource,
    SecureString,
    SnowflakeDataset,
    SnowflakeLinkedService,
)
from azure.mgmt.storage import StorageManagementClient

from utils import config
from utils.azure import get_azure_credential
from utils.logger import get_logger

logger = get_logger(__name__)

RAW_TABLE_NAME = "raw_sales_data"
SCHEMA_RAW = "RAW"


def create_data_factory_if_not_exists(
    client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    location: str = "eastus",
) -> Factory:
    """Create a data factory if it doesn't already exist."""
    try:
        logger.info("Checking if Data Factory %s exists...", factory_name)
        factory = client.factories.get(resource_group, factory_name)
        logger.info("Data Factory %s already exists", factory_name)
    except ResourceNotFoundError:
        logger.info("Creating Data Factory %s in %s...", factory_name, location)
        factory_resource = Factory(location=location)
        factory = client.factories.create_or_update(resource_group, factory_name, factory_resource)
        logger.info("Data Factory %s created successfully", factory_name)
        # Allow time for provisioning
        logger.info("Waiting for Data Factory to be fully provisioned...")
        time.sleep(30)

    return factory


def create_blob_linked_service(
    client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    azure_details: dict[str, str],
    credential: ClientSecretCredential,
) -> None:
    """Create and register an Azure Blob Storage linked service."""
    logger.info("Creating Azure Blob Storage linked service...")

    storage_client = StorageManagementClient(credential, azure_details["subscription_id"])
    keys = storage_client.storage_accounts.list_keys(
        azure_details["resource_group"],
        azure_details["storage_account"],
    )
    storage_key = keys.keys[0].value

    blob_linked_service = LinkedServiceResource(
        properties=AzureBlobStorageLinkedService(
            connection_string=f"DefaultEndpointsProtocol=https;AccountName={azure_details['storage_account']};AccountKey={storage_key};EndpointSuffix=core.windows.net",
        ),
    )

    client.linked_services.create_or_update(
        resource_group,
        factory_name,
        "AzureBlobStorage",
        blob_linked_service,
    )
    logger.info("Azure Blob Storage linked service created")


def create_snowflake_linked_service(
    client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    snowflake_details: dict[str, str],
) -> None:
    """Create and register a Snowflake linked service."""
    logger.info("Creating Snowflake linked service...")
    snowflake_linked_service = LinkedServiceResource(
        properties=SnowflakeLinkedService(
            connection_string=f"jdbc:snowflake://{snowflake_details['account']}"
            f".snowflakecomputing.com/?user={snowflake_details['user']}"
            f"&warehouse={snowflake_details['warehouse']}"
            f"&db={snowflake_details['database']}&schema={SCHEMA_RAW}",
            password=SecureString(value=snowflake_details["password"]),
        ),
    )

    client.linked_services.create_or_update(
        resource_group,
        factory_name,
        "SnowflakeDB",
        snowflake_linked_service,
    )
    logger.info("Snowflake linked service created")


def create_datasets(
    client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    azure_details: dict[str, str],
) -> None:
    """Create source and target datasets."""
    logger.info("Creating datasets...")

    # Source dataset (CSV in blob storage)
    source_dataset = DatasetResource(
        properties=DelimitedTextDataset(
            linked_service_name=LinkedServiceReference(
                reference_name="AzureBlobStorage",
                type="LinkedServiceReference",
            ),
            location=AzureBlobStorageLocation(
                container=azure_details["container_name"],
                file_name=azure_details["blob_name"],
            ),
            column_delimiter=",",
            row_delimiter="\n",
            first_row_as_header=True,
        ),
    )

    # Target dataset (Snowflake raw table)
    target_dataset = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(
                reference_name="SnowflakeDB",
                type="LinkedServiceReference",
            ),
            schema=SCHEMA_RAW,
            table=RAW_TABLE_NAME,
        ),
    )

    # Register datasets
    client.datasets.create_or_update(
        resource_group,
        factory_name,
        "SalesCSV",
        source_dataset,
    )

    client.datasets.create_or_update(
        resource_group,
        factory_name,
        "RawSalesTable",
        target_dataset,
    )

    logger.info("Datasets created successfully")


def create_and_run_pipeline(
    client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
) -> str:
    """Create and run a pipeline to copy data from Blob Storage to Snowflake.

    Args:
        client: Data factory management client
        resource_group: Resource group name
        factory_name: Data factory name

    Returns:
        The pipeline run ID
    """
    logger.info("Creating copy pipeline...")
    pipeline = PipelineResource(
        activities=[
            CopyActivity(
                name="CopyToRawTable",
                type="Copy",
                inputs=[DatasetReference(reference_name="SalesCSV", type="DatasetReference")],
                outputs=[DatasetReference(reference_name="RawSalesTable", type="DatasetReference")],
                source=CopySource(
                    type="DelimitedTextSource",
                    store_settings=AzureBlobStorageReadSettings(
                        recursive=False,
                        enable_partition_discovery=False,
                    ),
                    format_settings=DelimitedTextReadSettings(
                        skip_line_count=0,
                        first_row_as_header=True,
                        delimiter=",",
                    ),
                ),
                sink=CopySink(type="SnowflakeSink", write_behavior="Insert"),
                enable_staging=False,
                policy=ActivityPolicy(),
                column_mappings={
                    "Region": "region",
                    "Country": "country",
                    "Item Type": "item_type",
                    "Sales Channel": "sales_channel",
                    "Order Priority": "order_priority",
                    "Order Date": "order_date",
                    "Order ID": "order_id",
                    "Ship Date": "ship_date",
                    "Units Sold": "units_sold",
                    "Unit Price": "unit_price",
                    "Unit Cost": "unit_cost",
                },
            ),
        ],
    )

    # Register the pipeline
    client.pipelines.create_or_update(
        resource_group,
        factory_name,
        "RawDataLoadPipeline",
        pipeline,
    )
    logger.info("Pipeline created successfully")

    # Run the pipeline
    logger.info("Starting pipeline execution...")
    run_response = client.pipelines.create_run(
        resource_group,
        factory_name,
        "RawDataLoadPipeline",
    )

    logger.info("Pipeline run ID: %s", run_response.run_id)
    return run_response.run_id


def main() -> None:
    """Main function to create and execute a data loading pipeline."""
    logger.info("Starting the creation of Azure Data Factory pipeline for raw data loading...")

    # Get Azure credentials and config
    azure_details = config.get_azure_details()
    snowflake_details = config.get_snowflake_details()
    credential = get_azure_credential()

    resource_group = azure_details["resource_group"]
    factory_name = azure_details["data_factory_name"]

    # Create ADF client
    adf_client = DataFactoryManagementClient(credential, azure_details["subscription_id"])

    # Create Data Factory if it doesn't exist
    create_data_factory_if_not_exists(adf_client, resource_group, factory_name)

    # Create linked services
    create_blob_linked_service(adf_client, resource_group, factory_name, azure_details, credential)
    create_snowflake_linked_service(adf_client, resource_group, factory_name, snowflake_details)

    # Create datasets
    create_datasets(adf_client, resource_group, factory_name, azure_details)

    # Create and run pipeline
    run_id = create_and_run_pipeline(adf_client, resource_group, factory_name)

    logger.info("Data pipeline setup complete!")

    return run_id


if __name__ == "__main__":
    main()
