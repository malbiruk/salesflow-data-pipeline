"""
This script does the following:
1. downloads the dataset from
https://excelbianalytics.com/wp/wp-content/uploads/2017/07/10000-Sales-Records.zip
2. extracts .csv and removes the .zip file
3. using Azure Servie Principal automatically creates resource group, storage account, and container inside it
4. uploads the .csv in blob into the created container
"""

import logging
import os
import sys
import zipfile
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from rich.logging import RichHandler

from logger import get_logger

logger = get_logger()

# Load environment variables from .env file
load_dotenv()

FILE_NAME = "10000 Sales Records.csv"
BLOB_NAME = "10000 Sales Records.csv"


def check_env_vars():
    """Check if all required environment variables are set."""
    required_vars = [
        "AZURE_APP_ID",
        "AZURE_PASSWORD",
        "AZURE_TENANT",
        "AZURE_RESOURCE_GROUP",
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_CONTAINER_NAME",
        "AZURE_SUBSCRIPTION_ID",
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error("Error: Missing environment variables: %s", ", ".join(missing_vars))
        sys.exit(1)


def download_dataset():
    """Download and extract the sales dataset."""
    if Path(FILE_NAME).exists():
        logger.info("Dataset was already downloaded.")
        return

    logger.info("Downloading dataset...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }

    response = requests.get(
        "https://excelbianalytics.com/wp/wp-content/uploads/2017/07/10000-Sales-Records.zip",
        headers=headers,
        stream=True,
    )

    response.raise_for_status()

    zip_path = Path("sales-dataset.zip")

    with zip_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(".")

    zip_path.unlink()
    logger.info("Dataset downloaded and extracted.")


def get_azure_credential():
    """Get Azure credential using service principal."""
    logger.info("Authenticating with Azure...")
    return ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT"),
        client_id=os.getenv("AZURE_APP_ID"),
        client_secret=os.getenv("AZURE_PASSWORD"),
    )


def create_azure_resources(credential):
    """Create necessary Azure resources if they don't exist."""
    logger.info("Creating Azure resources...")

    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_RESOURCE_GROUP")
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container_name = os.getenv("AZURE_CONTAINER_NAME")

    # Create resource client
    resource_client = ResourceManagementClient(credential, subscription_id)

    # Create resource group if it doesn't exist
    if not any(rg.name == resource_group for rg in resource_client.resource_groups.list()):
        logger.info("Creating resource group %s...", resource_group)
        resource_client.resource_groups.create_or_update(resource_group, {"location": "uaenorth"})

    # Create storage client
    storage_client = StorageManagementClient(credential, subscription_id)

    # Create storage account if it doesn't exist
    if not any(
        sa.name == storage_account
        for sa in storage_client.storage_accounts.list_by_resource_group(resource_group)
    ):
        logger.info("Creating storage account %s...", storage_account)
        storage_client.storage_accounts.begin_create(
            resource_group,
            storage_account,
            {"location": "uaenorth", "kind": "StorageV2", "sku": {"name": "Standard_LRS"}},
        ).result()

    # Get storage account keys
    keys = storage_client.storage_accounts.list_keys(resource_group, storage_account)
    storage_key = keys.keys[0].value

    # Create a blob service client
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=storage_key,
    )

    # Create container if it doesn't exist
    if not any(
        container.name == container_name for container in blob_service_client.list_containers()
    ):
        logger.info("Creating container %s...", container_name)
        blob_service_client.create_container(container_name)

    return blob_service_client


def upload_to_blob(blob_service_client):
    """Upload the dataset to Azure Blob Storage."""
    logger.info("Uploading dataset to Azure Blob...")

    container_name = os.getenv("AZURE_CONTAINER_NAME")
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(BLOB_NAME)

    with Path(FILE_NAME).open("rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logger.info(
        "File '%s' uploaded to '%s' in container '%s'",
        FILE_NAME,
        BLOB_NAME,
        container_name,
    )


def main():
    """Main function to orchestrate the workflow."""
    check_env_vars()
    download_dataset()
    credential = get_azure_credential()
    blob_service_client = create_azure_resources(credential)
    upload_to_blob(blob_service_client)
    logger.info("Upload process completed successfully.")


if __name__ == "__main__":
    main()
