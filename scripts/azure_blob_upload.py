"""
This script does the following:
1. downloads the dataset from
https://excelbianalytics.com/wp/wp-content/uploads/2017/07/10000-Sales-Records.zip
2. extracts .csv and removes the .zip file
3. using Azure Servie Principal automatically creates resource group, storage account,
and container inside it
4. uploads the .csv in blob into the created container
"""

import os
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    ResourceTypes,
    generate_account_sas,
)

from utils import config
from utils.azure import get_azure_credential
from utils.logger import get_logger

logger = get_logger()

FILE_NAME = "10000 Sales Records.csv"


def generate_sas_token(storage_account: str, storage_key: str) -> str:
    """Generate a SAS token for the entire storage account."""
    logger.info("Generating SAS token for storage account %s...", storage_account)

    # Generate a SAS token for the entire account (not just one container)
    sas_token = generate_account_sas(
        account_name=storage_account,
        account_key=storage_key,
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(read=True, write=True, list=True),
        expiry=datetime.now(tz=UTC) + timedelta(days=10),
    )

    logger.info("SAS token generated successfully")
    return sas_token


def download_dataset() -> None:
    """Download and extract the sales dataset."""
    if Path(f"data/{FILE_NAME}").exists():
        logger.info("Dataset was already downloaded.")
        return

    logger.info("Downloading dataset...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }

    response = requests.get(
        "https://excelbianalytics.com/wp/wp-content/uploads/2017/07/10000-Sales-Records.zip",
        headers=headers,
        stream=True,
        timeout=30,
    )

    response.raise_for_status()

    zip_path = Path("sales-dataset.zip")

    with zip_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall("data/")

    zip_path.unlink()
    logger.info("Dataset downloaded and extracted.")


def add_sas_token_to_dotenv(sas_token: str) -> None:
    env_path = Path(".env")

    env_content = ""
    if env_path.exists():
        env_content = env_path.read_text()

    if "AZURE_SAS_TOKEN=" in env_content:
        lines = env_content.splitlines()
        updated_lines = []
        for line in lines:
            if line.startswith("AZURE_SAS_TOKEN="):
                updated_lines.append(f'AZURE_SAS_TOKEN="{sas_token}"')
            else:
                updated_lines.append(line)
        env_path.write_text("\n".join(updated_lines))
    else:
        with env_path.open("a") as f:
            if env_content and not env_content.endswith("\n"):
                f.write("\n")
            f.write(f'AZURE_SAS_TOKEN="{sas_token}"\n')


def create_azure_resources(credential: ClientSecretCredential) -> BlobServiceClient:
    """Create necessary Azure resources if they don't exist."""
    logger.info("Creating Azure resources...")

    azure_details = config.get_azure_details()
    subscription_id = azure_details["subscription_id"]
    resource_group = azure_details["resource_group"]
    storage_account = azure_details["storage_account"]
    container_name = azure_details["container_name"]

    resource_client = ResourceManagementClient(credential, subscription_id)
    if not any(rg.name == resource_group for rg in resource_client.resource_groups.list()):
        logger.info("Creating resource group %s...", resource_group)
        resource_client.resource_groups.create_or_update(resource_group, {"location": "uaenorth"})

    storage_client = StorageManagementClient(credential, subscription_id)
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

    keys = storage_client.storage_accounts.list_keys(resource_group, storage_account)
    storage_key = keys.keys[0].value
    sas_token = generate_sas_token(storage_account, storage_key)
    add_sas_token_to_dotenv(sas_token)
    os.environ["AZURE_SAS_TOKEN"] = sas_token

    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=storage_key,
    )
    if not any(
        container.name == container_name for container in blob_service_client.list_containers()
    ):
        logger.info("Creating container %s...", container_name)
        blob_service_client.create_container(container_name)

    return blob_service_client


def upload_to_blob(blob_service_client: BlobServiceClient) -> None:
    """Upload the dataset to Azure Blob Storage."""
    logger.info("Uploading dataset to Azure Blob...")

    azure_details = config.get_azure_details()
    container_name = azure_details["container_name"]
    blob_name = azure_details["blob_name"]
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / FILE_NAME).open("r", encoding="utf-8") as input_file:
        next(input_file)  # skipping header line for simplicity of loading into Snowflake later

        content = input_file.read()
        blob_client.upload_blob(content, overwrite=True)

    logger.info(
        "File '%s' uploaded to '%s' in container '%s'",
        FILE_NAME,
        blob_name,
        container_name,
    )


def main() -> None:
    """Main function to orchestrate the workflow."""
    download_dataset()
    credential = get_azure_credential()
    blob_service_client = create_azure_resources(credential)
    upload_to_blob(blob_service_client)
    logger.info("Upload process completed successfully.")


if __name__ == "__main__":
    main()
