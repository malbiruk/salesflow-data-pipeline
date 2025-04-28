"""Script to load data from Azure Blob Storage to Snowflake RAW schema."""

import time

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    AzureBlobStorageLocation,
    DatasetResource,
    DelimitedTextDataset,
    Factory,
    LinkedServiceReference,
    LinkedServiceResource,
    PipelineResource,
)

from utils import config
from utils.azure import get_azure_credential
from utils.logger import get_logger

logger = get_logger(__name__)

RAW_TABLE_NAME = "RAW_SALES_DATA"
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
) -> None:
    """Create and register an Azure Blob Storage linked service."""
    logger.info("Creating Azure Blob Storage linked service...")

    # Use SAS authentication instead of connection string
    blob_linked_service = LinkedServiceResource(
        properties={
            "type": "AzureBlobStorage",
            "typeProperties": {
                "sasUri": f"https://{azure_details['storage_account']}.blob.core.windows.net/?{azure_details['sas_token']}",
            },
        },
    )

    client.linked_services.create_or_update(
        resource_group,
        factory_name,
        "AzureBlobStorage",
        blob_linked_service,
    )
    logger.info("Azure Blob Storage linked service with SAS created")


def create_snowflake_linked_service(
    client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    snowflake_details: dict[str, str],
) -> None:
    """Create and register a Snowflake V2 linked service."""
    logger.info("Creating Snowflake V2 linked service...")

    snowflake_linked_service = LinkedServiceResource(
        properties={
            "type": "SnowflakeV2",
            "typeProperties": {
                "accountIdentifier": snowflake_details["account"],
                "warehouse": snowflake_details["warehouse"],
                "database": snowflake_details["database"],
                "schema": SCHEMA_RAW,
                "authenticationType": "Basic",
                "user": snowflake_details["user"],
                "password": {"type": "SecureString", "value": snowflake_details["password"]},
            },
        },
    )

    client.linked_services.create_or_update(
        resource_group,
        factory_name,
        "SnowflakeDB",
        snowflake_linked_service,
    )
    logger.info("Snowflake V2 linked service created")


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
        ),
    )

    # Target dataset (Snowflake raw table)
    target_dataset = DatasetResource(
        properties={
            "type": "SnowflakeV2Table",
            "linkedServiceName": {"referenceName": "SnowflakeDB", "type": "LinkedServiceReference"},
            "typeProperties": {"table": RAW_TABLE_NAME, "schema": SCHEMA_RAW},
        },
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
    """Create and run a pipeline to copy data from Blob Storage to Snowflake."""
    logger.info("Creating copy pipeline...")
    pipeline = PipelineResource(
        activities=[
            {
                "name": "CopyToSnowflake",
                "type": "Copy",
                "inputs": [{"referenceName": "SalesCSV", "type": "DatasetReference"}],
                "outputs": [{"referenceName": "RawSalesTable", "type": "DatasetReference"}],
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "AzureBlobStorageReadSettings",
                            "enablePartitionDiscovery": False,
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings",
                            "skipLineCount": 0,
                        },
                    },
                    "sink": {
                        "type": "SnowflakeV2Sink",
                        "importSettings": {
                            "type": "SnowflakeImportCopyCommand",
                            "additionalCopyOptions": {"ON_ERROR": "CONTINUE"},
                        },
                    },
                    "enableStaging": False,
                    "translator": {
                        "type": "TabularTranslator",
                        "mappings": [
                            {"source": {"name": "Prop_0"}, "sink": {"name": "REGION"}},
                            {"source": {"name": "Prop_1"}, "sink": {"name": "COUNTRY"}},
                            {"source": {"name": "Prop_2"}, "sink": {"name": "ITEM_TYPE"}},
                            {"source": {"name": "Prop_3"}, "sink": {"name": "SALES_CHANNEL"}},
                            {"source": {"name": "Prop_4"}, "sink": {"name": "ORDER_PRIORITY"}},
                            {"source": {"name": "Prop_5"}, "sink": {"name": "ORDER_DATE"}},
                            {"source": {"name": "Prop_6"}, "sink": {"name": "ORDER_ID"}},
                            {"source": {"name": "Prop_7"}, "sink": {"name": "SHIP_DATE"}},
                            {"source": {"name": "Prop_8"}, "sink": {"name": "UNITS_SOLD"}},
                            {"source": {"name": "Prop_9"}, "sink": {"name": "UNIT_PRICE"}},
                            {"source": {"name": "Prop_10"}, "sink": {"name": "UNIT_COST"}},
                        ],
                    },
                },
            },
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
    create_blob_linked_service(adf_client, resource_group, factory_name, azure_details)
    create_snowflake_linked_service(adf_client, resource_group, factory_name, snowflake_details)

    # Create datasets
    create_datasets(adf_client, resource_group, factory_name, azure_details)

    # Create and run pipeline
    run_id = create_and_run_pipeline(adf_client, resource_group, factory_name)

    logger.info("Data pipeline setup complete!")

    return run_id


if __name__ == "__main__":
    main()
