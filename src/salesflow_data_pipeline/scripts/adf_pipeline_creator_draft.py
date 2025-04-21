import os

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityPolicy,
    AzureBlobStorageLinkedService,
    AzureBlobStorageLocation,
    DataFlowReference,
    DataFlowResource,
    DataFlowSink,
    DataFlowSource,
    DatasetReference,
    DatasetResource,
    DelimitedTextDataset,
    Distinct,
    ExecuteDataFlowActivity,
    LinkedServiceReference,
    LinkedServiceResource,
    MappingDataFlow,
    PipelineResource,
    SecureString,
    Select,
    SnowflakeDataset,
    SnowflakeLinkedService,
)
from azure_blob_upload import get_azure_credential
from dotenv import load_dotenv
from logger import get_logger

logger = get_logger()

load_dotenv()


def create_linked_services(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    data_factory_name: str,
) -> None:
    """Create and register linked services for source and sink."""
    logger.info("Creating linked services...")

    # Create blob storage linked service
    azure_blob_linked_service = LinkedServiceResource(
        properties=AzureBlobStorageLinkedService(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        ),
    )

    # Create Snowflake linked service
    snowflake_linked_service = LinkedServiceResource(
        properties=SnowflakeLinkedService(
            connection_string=f"account={os.getenv('SNOWFLAKE_ACCOUNT')};",
            authentication_type="Basic",
            user_name=os.getenv("SNOWFLAKE_USER"),
            password=SecureString(value=os.getenv("SNOWFLAKE_PASSWORD")),
        ),
    )

    # Register linked services in Azure
    adf_client.linked_services.create_or_update(
        resource_group,
        data_factory_name,
        "AzureBlobStorage",
        azure_blob_linked_service,
    )

    adf_client.linked_services.create_or_update(
        resource_group,
        data_factory_name,
        "SnowflakeDB",
        snowflake_linked_service,
    )

    logger.info("Linked services created successfully")


def create_datasets(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    data_factory_name: str,
) -> None:
    """Create and register all datasets."""
    logger.info("Creating datasets...")

    # Source dataset (CSV in blob storage)
    source_dataset = DatasetResource(
        properties=DelimitedTextDataset(
            linked_service_name=LinkedServiceReference(reference_name="AzureBlobStorage"),
            location=AzureBlobStorageLocation(
                container=os.getenv("AZURE_CONTAINER_NAME"),
                file_name=os.getenv("BLOB_NAME"),
            ),
            column_delimiter=",",
            row_delimiter="\n",
            first_row_as_header=True,
        ),
    )

    # Target datasets (Snowflake tables)
    region_dataset = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="region",
        ),
    )

    country_dataset = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="country",
        ),
    )

    product_dataset = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="product",
        ),
    )

    order_priority_dataset = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="order_priority",
        ),
    )

    order_dataset = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="order",
        ),
    )

    # Register datasets in Azure
    adf_client.datasets.create_or_update(
        resource_group,
        data_factory_name,
        "SalesCSV",
        source_dataset,
    )
    adf_client.datasets.create_or_update(
        resource_group,
        data_factory_name,
        "RegionTable",
        region_dataset,
    )
    adf_client.datasets.create_or_update(
        resource_group,
        data_factory_name,
        "CountryTable",
        country_dataset,
    )
    adf_client.datasets.create_or_update(
        resource_group,
        data_factory_name,
        "ProductTable",
        product_dataset,
    )
    adf_client.datasets.create_or_update(
        resource_group,
        data_factory_name,
        "OrderPriorityTable",
        order_priority_dataset,
    )
    adf_client.datasets.create_or_update(
        resource_group,
        data_factory_name,
        "OrderTable",
        order_dataset,
    )

    logger.info("Datasets created successfully")


def create_data_flow(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    data_factory_name: str,
) -> None:
    """Create and register the data flow for transformations."""
    logger.info("Creating data flow...")

    # Define the complete data flow
    data_flow = DataFlowResource(
        properties=MappingDataFlow(
            sources=[DataFlowSource(name="SalesSource", dataset=DatasetReference("SalesCSV"))],
            sinks=[
                DataFlowSink(name="RegionSink", dataset=DatasetReference("RegionTable")),
                DataFlowSink(name="CountrySink", dataset=DatasetReference("CountryTable")),
                DataFlowSink(name="ProductSink", dataset=DatasetReference("ProductTable")),
                DataFlowSink(
                    name="OrderPrioritySink",
                    dataset=DatasetReference("OrderPriorityTable"),
                ),
                DataFlowSink(name="OrderSink", dataset=DatasetReference("OrderTable")),
            ],
            transformations=[
                # Region transformations
                Select(name="SelectRegionColumns", schema=[{"name": "Region", "type": "string"}]),
                Distinct(name="DistinctRegions", columns=["Region"]),
                # Country transformations
                Select(
                    name="SelectCountryColumns",
                    schema=[
                        {"name": "Region", "type": "string"},
                        {"name": "Country", "type": "string"},
                    ],
                ),
                Distinct(name="DistinctCountries", columns=["Country", "Region"]),
                # Product transformations
                Select(
                    name="SelectProductColumns",
                    schema=[
                        {"name": "Item Type", "type": "string"},
                        {"name": "Unit Cost", "type": "double"},
                        {"name": "Unit Price", "type": "double"},
                    ],
                ),
                Distinct(name="DistinctProducts", columns=["Item Type"]),
                # Order Priority transformations
                Select(
                    name="SelectOrderPriorityColumns",
                    schema=[{"name": "Order Priority", "type": "string"}],
                ),
                Distinct(name="DistinctOrderPriorities", columns=["Order Priority"]),
                # Orders transformations
                Select(
                    name="SelectOrderColumns",
                    schema=[
                        {"name": "Order ID", "type": "integer"},
                        {"name": "Country", "type": "string"},
                        {"name": "Sales Channel", "type": "string"},
                        {"name": "Order Priority", "type": "string"},
                        {"name": "Item Type", "type": "string"},
                        {"name": "Units Sold", "type": "integer"},
                        {"name": "Order Date", "type": "date"},
                        {"name": "Ship Date", "type": "date"},
                    ],
                ),
                # Add more transformations as needed for the order table
            ],
        ),
    )

    # Register data flow in Azure
    adf_client.data_flows.create_or_update(
        resource_group,
        data_factory_name,
        "SalesDataFlow",
        data_flow,
    )

    logger.info("Data flow created successfully")


def create_pipeline(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    data_factory_name: str,
) -> None:
    """Create and register the pipeline that executes the data flow."""
    logger.info("Creating pipeline...")

    # Define the pipeline with the data flow activity
    pipeline = PipelineResource(
        activities=[
            ExecuteDataFlowActivity(
                name="TransformSalesData",
                type="ExecuteDataFlow",
                depends_on=[],
                policy=ActivityPolicy(),
                data_flow=DataFlowReference(reference_name="SalesDataFlow"),
            ),
        ],
    )

    # Register the pipeline in Azure
    adf_client.pipelines.create_or_update(
        resource_group,
        data_factory_name,
        "SalesTransformationPipeline",
        pipeline,
    )

    logger.info("Pipeline created successfully")


def main() -> None:
    """Main function to orchestrate the creation of the ADF pipeline."""
    logger.info("Starting the creation of Azure Data Factory pipeline...")

    # Get Azure credentials and config
    credential = get_azure_credential()
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_RESOURCE_GROUP")
    data_factory_name = os.getenv("AZURE_DATA_FACTORY_NAME")

    # Create ADF client
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Create components in sequence
    create_linked_services(adf_client, resource_group, data_factory_name)
    create_datasets(adf_client, resource_group, data_factory_name)
    create_data_flow(adf_client, resource_group, data_factory_name)
    create_pipeline(adf_client, resource_group, data_factory_name)

    logger.info("Azure Data Factory pipeline creation completed successfully!")


if __name__ == "__main__":
    main()
