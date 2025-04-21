from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import DataFlow, DatasetResource, LinkedServiceResource, Pipeline

from salesflow_data_pipeline.utils.logger import get_logger

logger = get_logger()


class ADFDeployer:
    """Azure Data Factory resource deployer."""

    def __init__(
        self,
        adf_client: DataFactoryManagementClient,
        resource_group: str,
        factory_name: str,
    ) -> None:
        """Initialize deployer with common ADF client and resource identifiers."""
        self.adf_client = adf_client
        self.resource_group = resource_group
        self.factory_name = factory_name
        logger.info("Initialized deployer for data factory: %s", factory_name)

    def deploy_linked_services(self, linked_services: tuple[LinkedServiceResource]) -> None:
        """Deploy linked services to Azure Data Factory."""
        azure_blob_linked_service, snowflake_linked_service = linked_services

        self.adf_client.linked_services.create_or_update(
            self.resource_group,
            self.factory_name,
            "AzureBlobStorage",
            azure_blob_linked_service,
        )

        self.adf_client.linked_services.create_or_update(
            self.resource_group,
            self.factory_name,
            "SnowflakeDB",
            snowflake_linked_service,
        )

        logger.info("Linked services deployed successfully")

    def deploy_datasets(self, datasets: dict[str, DatasetResource]) -> None:
        """Deploy datasets to Azure Data Factory."""
        for dataset_name, dataset in datasets.items():
            self.adf_client.datasets.create_or_update(
                self.resource_group,
                self.factory_name,
                dataset_name,
                dataset,
            )

        logger.info("Deployed %s datasets successfully", len(datasets))

    def deploy_all_resources(
        self,
        linked_services: tuple[LinkedServiceResource],
        datasets: dict[str, DatasetResource],
        data_flow: DataFlow,
        pipeline: Pipeline,
    ) -> None:
        """Deploy all ADF resources in the correct order."""
        self.deploy_linked_services(linked_services)
        self.deploy_datasets(datasets)

        logger.info("All resources deployed successfully")
