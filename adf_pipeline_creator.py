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
from dotenv import load_dotenv

from azure_blob_upload import get_azure_credential
from logger import get_logger

logger = get_logger()

load_dotenv()


def main():
    credential = get_azure_credential()
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_RESOURCE_GROUP")
    data_factory_name = os.getenv("AZURE_DATA_FACTORY_NAME")

    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Create linked services for source (Blob) and sink (Snowflake)
    azure_blob_linked_service = LinkedServiceResource(
        properties=AzureBlobStorageLinkedService(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        ),
    )

    snowflake_linked_service = LinkedServiceResource(
        properties=SnowflakeLinkedService(
            connection_string=f"account={os.getenv('SNOWFLAKE_ACCOUNT')};",
            authentication_type="Basic",
            user_name=os.getenv("SNOWFLAKE_USER"),
            password=SecureString(value=os.getenv("SNOWFLAKE_PASSWORD")),
        ),
    )

    # Create datasets
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

    # Create datasets for each target table
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

    # ... other datasets for product, order_priority, order

    # Create data flow for transformation
    data_flow = DataFlowResource(
        properties=MappingDataFlow(
            sources=[DataFlowSource(name="SalesSource", dataset=DatasetReference("SalesCSV"))],
            sinks=[
                DataFlowSink(name="RegionSink", dataset=DatasetReference("RegionTable")),
                DataFlowSink(name="CountrySink", dataset=DatasetReference("CountryTable")),
                # ... other sinks
            ],
            transformations=[
                # Extract unique regions
                Select(name="SelectRegion", schema=[{"name": "Region", "type": "string"}]),
                Distinct(name="DistinctRegions", columns=["Region"]),
                # Extract Region-Country pairs for country table
                Select(
                    name="SelectCountry",
                    schema=[
                        {"name": "Region", "type": "string"},
                        {"name": "Country", "type": "string"},
                    ],
                ),
                Distinct(name="DistinctCountries", columns=["Country", "Region"]),
                # ... other transformations for remaining tables
            ],
        ),
    )

    # Create pipeline with activities
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

    # Create the resources in Azure
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

    # ... create datasets and data flows

    adf_client.pipelines.create_or_update(
        resource_group,
        data_factory_name,
        "SalesTransformationPipeline",
        pipeline,
    )

    print("Azure Data Factory pipeline created successfully!")


if __name__ == "__main__":
    main()
