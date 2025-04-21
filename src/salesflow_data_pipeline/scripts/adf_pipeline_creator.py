from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from logger import get_logger

from salesflow_data_pipeline.adf.data_flows import create_data_flow
from salesflow_data_pipeline.adf.datasets import create_datasets
from salesflow_data_pipeline.adf.deployment import ADFDeployer
from salesflow_data_pipeline.adf.linked_services import create_linked_services
from salesflow_data_pipeline.adf.pipelines import create_pipeline
from salesflow_data_pipeline.utils import config

logger = get_logger()


def main() -> None:
    """Main entry point for ADF pipeline creation."""
    try:
        logger.info("Starting ADF pipeline creation process")

        azure_creds = config.get_azure_credentials()
        azure_details = config.get_azure_details()

        credential = ClientSecretCredential(**azure_creds)
        adf_client = DataFactoryManagementClient(credential, azure_details["subscription_id"])

        linked_services = create_linked_services()
        datasets = create_datasets()
        data_flow = create_data_flow()
        pipeline = create_pipeline()

        deployer = ADFDeployer(
            adf_client=adf_client,
            resource_group=azure_details["resource_group"],
            factory_name=azure_details["data_factory_name"],
        )

        deployer.deploy_all_resources(
            linked_services=linked_services,
            datasets=datasets,
            data_flow=data_flow,
            pipeline=pipeline,
        )

        logger.info("ADF pipeline creation completed successfully")

    except Exception:
        logger.exception("Error creating ADF pipeline: %s")
        raise


if __name__ == "__main__":
    main()
