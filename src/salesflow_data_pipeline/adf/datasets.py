from azure.mgmt.datafactory.models import (
    AzureBlobStorageLocation,
    DatasetResource,
    DelimitedTextDataset,
    LinkedServiceReference,
    SnowflakeDataset,
)

from salesflow_data_pipeline.utils import config
from salesflow_data_pipeline.utils.logger import get_logger

logger = get_logger()


def create_datasets() -> dict[str, DatasetResource]:
    """Create all datasets."""
    logger.info("Creating datasets...")

    storage_details = config.get_storage_details()
    datasets = {}

    # Source dataset (CSV in blob storage)
    datasets["SalesCSV"] = DatasetResource(
        properties=DelimitedTextDataset(
            linked_service_name=LinkedServiceReference(reference_name="AzureBlobStorage"),
            location=AzureBlobStorageLocation(
                container=storage_details["container_name"],
                file_name=storage_details["blob_name"],
            ),
            column_delimiter=",",
            row_delimiter="\n",
            first_row_as_header=True,
        ),
    )

    # Target datasets (Snowflake tables)
    datasets["RegionTable"] = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="region",
        ),
    )

    datasets["CountryTable"] = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="country",
        ),
    )
    datasets["ProductTable"] = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="product",
        ),
    )

    datasets["OrderPriorityTable"] = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="order_priority",
        ),
    )

    datasets["OrderTable"] = DatasetResource(
        properties=SnowflakeDataset(
            linked_service_name=LinkedServiceReference(reference_name="SnowflakeDB"),
            table="order",
        ),
    )

    return datasets
