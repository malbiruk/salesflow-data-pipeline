from unittest.mock import MagicMock, mock_open, patch

import pytest
import requests
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

from scripts.azure_blob_upload import (
    add_sas_token_to_dotenv,
    create_azure_resources,
    download_dataset,
    generate_sas_token,
    main,
    upload_to_blob,
)


@pytest.fixture
def mock_azure_details():
    """Fixture for Azure configuration details."""
    return {
        "subscription_id": "test-subscription",
        "resource_group": "test-rg",
        "storage_account": "teststorage",
        "container_name": "test-container",
        "blob_name": "test-blob.csv",
        "client_id": "test-client-id",
        "client_secret": "test-secret",
        "tenant_id": "test-tenant",
    }


@pytest.fixture
def mock_blob_service_client():
    """Fixture for Azure Blob Service Client."""
    client = MagicMock(spec=BlobServiceClient)

    # Setup container client mock
    container_client = MagicMock()
    client.get_container_client.return_value = container_client

    # Setup blob client mock
    blob_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client

    return client


@pytest.fixture
def mock_credential():
    """Fixture for Azure credential."""
    return MagicMock(spec=ClientSecretCredential)


class TestDownloadDataset:
    """Tests for download_dataset function."""

    @patch("scripts.azure_blob_upload.Path.exists")
    @patch("scripts.azure_blob_upload.requests.get")
    @patch("scripts.azure_blob_upload.Path.open", new_callable=mock_open)
    @patch("scripts.azure_blob_upload.zipfile.ZipFile")
    @patch("scripts.azure_blob_upload.Path.unlink")
    def test_download_dataset_when_not_exist(
        self,
        mock_unlink,
        mock_zipfile,
        mock_file,
        mock_get,
        mock_exists,
    ):
        """Test downloading the dataset when it doesn't exist locally."""
        # Setup
        mock_exists.return_value = False
        mock_response = MagicMock(spec=requests.Response)
        mock_response.iter_content.return_value = [b"test data"]
        mock_get.return_value = mock_response

        # Execute
        download_dataset()

        # Assert
        mock_get.assert_called_once()
        mock_file.assert_called()
        mock_zipfile.assert_called_once()
        mock_unlink.assert_called_once()

    @patch("scripts.azure_blob_upload.Path.exists")
    def test_download_dataset_when_exist(self, mock_exists):
        """Test downloading the dataset when it already exists locally."""
        # Setup
        mock_exists.return_value = True

        # Execute
        download_dataset()

        # Assert
        mock_exists.assert_called_once()

    @patch("scripts.azure_blob_upload.Path.exists")
    @patch("scripts.azure_blob_upload.requests.get")
    def test_download_dataset_http_error(self, mock_get, mock_exists):
        """Test handling HTTP error during download."""
        # Setup
        mock_exists.return_value = False
        mock_get.side_effect = requests.exceptions.HTTPError("404 Client Error")

        # Execute & Assert
        with pytest.raises(requests.exceptions.HTTPError):
            download_dataset()


class TestSasTokenGeneration:
    """Tests for SAS token generation functionality."""

    @patch("scripts.azure_blob_upload.generate_account_sas")
    def test_generate_sas_token(self, mock_generate_sas):
        """Test generating a SAS token."""
        # Setup
        mock_generate_sas.return_value = "test-sas-token"
        storage_account = "teststorage"
        storage_key = "test-key"

        # Execute
        result = generate_sas_token(storage_account, storage_key)

        # Assert
        mock_generate_sas.assert_called_once()
        assert result == "test-sas-token"

    @patch("scripts.azure_blob_upload.Path.exists")
    @patch("scripts.azure_blob_upload.Path.read_text")
    @patch("scripts.azure_blob_upload.Path.write_text")
    def test_add_sas_token_to_dotenv_existing_token(
        self,
        mock_write_text,
        mock_read_text,
        mock_exists,
    ):
        """Test adding SAS token to .env when token already exists."""
        # Setup
        mock_exists.return_value = True
        mock_read_text.return_value = "AZURE_SAS_TOKEN=old-token\nOTHER_VAR=value"

        # Execute
        add_sas_token_to_dotenv("new-token")

        # Assert
        mock_write_text.assert_called_once_with('AZURE_SAS_TOKEN="new-token"\nOTHER_VAR=value')

    @patch("scripts.azure_blob_upload.Path.exists")
    @patch("scripts.azure_blob_upload.Path.read_text")
    @patch("scripts.azure_blob_upload.Path.open", new_callable=mock_open)
    def test_add_sas_token_to_dotenv_new_token(
        self,
        mock_file,
        mock_read_text,
        mock_exists,
    ):
        """Test adding SAS token to .env when token doesn't exist."""
        # Setup
        mock_exists.return_value = True
        mock_read_text.return_value = "OTHER_VAR=value"

        # Execute
        add_sas_token_to_dotenv("new-token")

        # Assert
        mock_file().write.assert_called_with('AZURE_SAS_TOKEN="new-token"\n')


class TestAzureResourceCreation:
    """Tests for Azure resource creation functionality."""

    @patch("scripts.azure_blob_upload.config.get_azure_details")
    @patch("scripts.azure_blob_upload.ResourceManagementClient")
    @patch("scripts.azure_blob_upload.StorageManagementClient")
    @patch("scripts.azure_blob_upload.generate_sas_token")
    @patch("scripts.azure_blob_upload.add_sas_token_to_dotenv")
    @patch("scripts.azure_blob_upload.BlobServiceClient")
    @patch("os.environ")
    def test_create_azure_resources_all_new(
        self,
        mock_environ,
        mock_blob_client_class,
        mock_add_sas,
        mock_generate_sas,
        mock_storage_client_class,
        mock_resource_client_class,
        mock_get_azure_details,
        mock_azure_details,
        mock_credential,
        mock_blob_service_client,
    ):
        """Test creating all Azure resources when none exist."""
        # Setup
        mock_get_azure_details.return_value = mock_azure_details

        # Resource group doesn't exist
        mock_resource_client = MagicMock()
        mock_resource_client.resource_groups.list.return_value = []
        mock_resource_client_class.return_value = mock_resource_client

        # Storage account doesn't exist
        mock_storage_client = MagicMock()
        mock_storage_client.storage_accounts.list_by_resource_group.return_value = []
        mock_storage_keys = MagicMock()
        mock_storage_keys.keys = [MagicMock()]
        mock_storage_keys.keys[0].value = "test-key"
        mock_storage_client.storage_accounts.list_keys.return_value = mock_storage_keys
        mock_storage_client_class.return_value = mock_storage_client

        # SAS token
        mock_generate_sas.return_value = "test-sas-token"

        # Container doesn't exist
        mock_blob_client_class.return_value = mock_blob_service_client
        mock_blob_service_client.list_containers.return_value = []

        # Execute
        result = create_azure_resources(mock_credential)

        # Assert
        mock_resource_client.resource_groups.create_or_update.assert_called_once()
        mock_storage_client.storage_accounts.begin_create.assert_called_once()
        mock_generate_sas.assert_called_once_with(
            mock_azure_details["storage_account"],
            "test-key",
        )
        mock_add_sas.assert_called_once_with("test-sas-token")
        mock_environ.__setitem__.assert_called_once_with("AZURE_SAS_TOKEN", "test-sas-token")
        mock_blob_service_client.create_container.assert_called_once_with(
            mock_azure_details["container_name"],
        )
        assert result == mock_blob_service_client

    @patch("scripts.azure_blob_upload.config.get_azure_details")
    @patch("scripts.azure_blob_upload.ResourceManagementClient")
    @patch("scripts.azure_blob_upload.StorageManagementClient")
    @patch("scripts.azure_blob_upload.generate_sas_token")
    @patch("scripts.azure_blob_upload.BlobServiceClient")
    def test_create_azure_resources_all_exist(
        self,
        mock_blob_client_class,
        mock_generate_sas,
        mock_storage_client_class,
        mock_resource_client_class,
        mock_get_azure_details,
        mock_azure_details,
        mock_credential,
        mock_blob_service_client,
    ):
        """Test when all Azure resources already exist."""
        # Setup
        mock_get_azure_details.return_value = mock_azure_details

        # Resource group exists
        mock_resource_group = MagicMock()
        mock_resource_group.name = mock_azure_details["resource_group"]
        mock_resource_client = MagicMock()
        mock_resource_client.resource_groups.list.return_value = [mock_resource_group]
        mock_resource_client_class.return_value = mock_resource_client

        # Storage account exists
        mock_storage_account = MagicMock()
        mock_storage_account.name = mock_azure_details["storage_account"]
        mock_storage_client = MagicMock()
        mock_storage_client.storage_accounts.list_by_resource_group.return_value = [
            mock_storage_account,
        ]
        mock_storage_keys = MagicMock()
        mock_storage_keys.keys = [MagicMock()]
        mock_storage_keys.keys[0].value = "test-key"
        mock_storage_client.storage_accounts.list_keys.return_value = mock_storage_keys
        mock_storage_client_class.return_value = mock_storage_client

        # SAS token
        mock_generate_sas.return_value = "test-sas-token"

        # Container exists
        mock_blob_client_class.return_value = mock_blob_service_client
        mock_container = MagicMock()
        mock_container.name = mock_azure_details["container_name"]
        mock_blob_service_client.list_containers.return_value = [mock_container]

        # Execute
        result = create_azure_resources(mock_credential)

        # Assert
        mock_resource_client.resource_groups.create_or_update.assert_not_called()
        mock_storage_client.storage_accounts.begin_create.assert_not_called()
        mock_blob_service_client.create_container.assert_not_called()
        assert result == mock_blob_service_client


class TestBlobUpload:
    """Tests for blob upload functionality."""

    @patch("scripts.azure_blob_upload.config.get_azure_details")
    @patch("scripts.azure_blob_upload.Path")
    def test_upload_to_blob(
        self,
        mock_path_class,
        mock_get_azure_details,
        mock_azure_details,
        mock_blob_service_client,
    ):
        """Test uploading a file to Azure Blob Storage."""
        # Setup
        mock_get_azure_details.return_value = mock_azure_details

        # Mock file handling
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = "csv content"
        mock_path_instance.__truediv__.return_value.open.return_value = mock_file

        # Setup container and blob client mocks
        container_client = mock_blob_service_client.get_container_client.return_value
        blob_client = container_client.get_blob_client.return_value

        # Execute
        upload_to_blob(mock_blob_service_client)

        # Assert
        mock_blob_service_client.get_container_client.assert_called_once_with(
            mock_azure_details["container_name"],
        )
        container_client.get_blob_client.assert_called_once_with(
            mock_azure_details["blob_name"],
        )
        blob_client.upload_blob.assert_called_once_with("csv content", overwrite=True)


class TestMainFunction:
    """Tests for the main function."""

    @patch("scripts.azure_blob_upload.download_dataset")
    @patch("scripts.azure_blob_upload.get_azure_credential")
    @patch("scripts.azure_blob_upload.create_azure_resources")
    @patch("scripts.azure_blob_upload.upload_to_blob")
    def test_main_function_orchestration(
        self,
        mock_upload,
        mock_create_resources,
        mock_get_credential,
        mock_download,
        mock_credential,
        mock_blob_service_client,
    ):
        """Test that main function orchestrates all the steps correctly."""
        # Setup
        mock_get_credential.return_value = mock_credential
        mock_create_resources.return_value = mock_blob_service_client

        # Execute
        main()

        # Assert
        mock_download.assert_called_once()
        mock_get_credential.assert_called_once()
        mock_create_resources.assert_called_once_with(mock_credential)
        mock_upload.assert_called_once_with(mock_blob_service_client)
