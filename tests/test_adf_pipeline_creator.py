from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.datafactory.models import Factory

from scripts.adf_pipeline_creator import (
    create_and_run_pipeline,
    create_blob_linked_service,
    create_data_factory_if_not_exists,
    create_datasets,
    create_snowflake_linked_service,
    main,
)


@pytest.fixture
def mock_azure_details():
    """Fixture for Azure configuration details."""
    return {
        "subscription_id": "test-subscription",
        "resource_group": "test-rg",
        "data_factory_name": "test-df",
        "storage_account": "teststorage",
        "container_name": "test-container",
        "blob_name": "test-blob.csv",
        "client_id": "test-client-id",
        "client_secret": "test-secret",
        "tenant_id": "test-tenant",
        "sas_token": "test-sas-token",
    }


@pytest.fixture
def mock_snowflake_details():
    """Fixture for Snowflake configuration details."""
    return {
        "account": "test-account",
        "user": "test-user",
        "password": "test-password",
        "database": "TEST_DB",
        "warehouse": "TEST_WH",
    }


@pytest.fixture
def mock_adf_client():
    """Fixture for ADF client with mocked methods."""
    client = MagicMock()

    # Mock factories operations
    client.factories.get = MagicMock()
    client.factories.create_or_update = MagicMock(return_value=Factory(name="test-df"))

    # Mock linked_services operations
    client.linked_services.create_or_update = MagicMock()

    # Mock datasets operations
    client.datasets.create_or_update = MagicMock()

    # Mock pipelines operations
    client.pipelines.create_or_update = MagicMock()
    run_response = MagicMock()
    run_response.run_id = "test-run-id"
    client.pipelines.create_run = MagicMock(return_value=run_response)

    return client


@pytest.fixture
def mock_storage_client():
    """Fixture for storage client with mocked methods."""
    client = MagicMock()

    # Mock list_keys method
    keys_response = MagicMock()
    keys_response.keys = [MagicMock()]
    keys_response.keys[0].value = "test-storage-key"
    client.storage_accounts.list_keys = MagicMock(return_value=keys_response)

    return client


class TestDataFactoryCreation:
    """Tests for data factory creation functionality."""

    def test_create_data_factory_when_exists(self, mock_adf_client):
        """Test data factory creation when it already exists."""
        # Setup
        mock_factory = Factory(name="existing-df")
        mock_adf_client.factories.get.return_value = mock_factory

        # Execute
        result = create_data_factory_if_not_exists(
            mock_adf_client,
            "test-rg",
            "test-df",
        )

        # Assert
        mock_adf_client.factories.get.assert_called_once_with("test-rg", "test-df")
        mock_adf_client.factories.create_or_update.assert_not_called()
        assert result == mock_factory

    def test_create_data_factory_when_not_exists(self, mock_adf_client):
        """Test data factory creation when it doesn't exist."""
        # Setup
        mock_adf_client.factories.get.side_effect = ResourceNotFoundError("Not found")
        mock_factory = Factory(name="new-df")
        mock_adf_client.factories.create_or_update.return_value = mock_factory

        # Execute
        with patch("time.sleep") as mock_sleep:  # Don't actually sleep in tests
            result = create_data_factory_if_not_exists(
                mock_adf_client,
                "test-rg",
                "test-df",
                "westus",
            )

        # Assert
        mock_adf_client.factories.get.assert_called_once_with("test-rg", "test-df")
        mock_adf_client.factories.create_or_update.assert_called_once()
        mock_sleep.assert_called_once_with(30)
        assert result == mock_factory


class TestLinkedServices:
    """Tests for linked services creation functionality."""

    def test_create_blob_linked_service(self, mock_adf_client, mock_azure_details):
        """Test creation of blob storage linked service."""
        # Make sure mock_azure_details contains the sas_token
        mock_azure_details["sas_token"] = "test-sas-token"  # noqa: S105

        # Execute
        create_blob_linked_service(
            mock_adf_client,
            mock_azure_details["resource_group"],
            mock_azure_details["data_factory_name"],
            mock_azure_details,
        )

        # Assert
        mock_adf_client.linked_services.create_or_update.assert_called_once()

    def test_create_snowflake_linked_service(self, mock_adf_client, mock_snowflake_details):
        """Test creation of Snowflake linked service."""
        # Execute
        create_snowflake_linked_service(
            mock_adf_client,
            "test-rg",
            "test-df",
            mock_snowflake_details,
        )

        # Assert
        mock_adf_client.linked_services.create_or_update.assert_called_once()
        # Here you could additionally check the args of the call to verify the service
        # was created with correct parameters


class TestDatasets:
    """Tests for dataset creation functionality."""

    def test_create_datasets(self, mock_adf_client, mock_azure_details):
        """Test creation of source and target datasets."""
        # Execute
        create_datasets(
            mock_adf_client,
            mock_azure_details["resource_group"],
            mock_azure_details["data_factory_name"],
            mock_azure_details,
        )

        # Assert
        assert mock_adf_client.datasets.create_or_update.call_count == 2
        # First call should create SalesCSV dataset
        first_call_args = mock_adf_client.datasets.create_or_update.call_args_list[0][0]
        assert first_call_args[2] == "SalesCSV"

        # Second call should create RawSalesTable dataset
        second_call_args = mock_adf_client.datasets.create_or_update.call_args_list[1][0]
        assert second_call_args[2] == "RawSalesTable"


class TestPipeline:
    """Tests for pipeline creation and execution."""

    def test_create_and_run_pipeline(self, mock_adf_client):
        """Test creation and execution of the copy pipeline."""
        # Execute
        run_id = create_and_run_pipeline(
            mock_adf_client,
            "test-rg",
            "test-df",
        )

        # Assert
        mock_adf_client.pipelines.create_or_update.assert_called_once()
        mock_adf_client.pipelines.create_run.assert_called_once()
        assert run_id == "test-run-id"


class TestMainFunction:
    """Tests for the main function."""

    @patch("scripts.adf_pipeline_creator.config")
    @patch("scripts.adf_pipeline_creator.get_azure_credential")
    @patch("scripts.adf_pipeline_creator.DataFactoryManagementClient")
    @patch("scripts.adf_pipeline_creator.create_data_factory_if_not_exists")
    @patch("scripts.adf_pipeline_creator.create_blob_linked_service")
    @patch("scripts.adf_pipeline_creator.create_snowflake_linked_service")
    @patch("scripts.adf_pipeline_creator.create_datasets")
    @patch("scripts.adf_pipeline_creator.create_and_run_pipeline")
    def test_main_function_orchestration(
        self,
        mock_create_pipeline,
        mock_create_datasets,
        mock_create_snowflake,
        mock_create_blob,
        mock_create_df,
        mock_adf_client_class,
        mock_get_cred,
        mock_config,
        mock_azure_details,
        mock_snowflake_details,
    ):
        """Test that main function orchestrates all the steps correctly."""
        # Setup
        mock_config.get_azure_details.return_value = mock_azure_details
        mock_config.get_snowflake_details.return_value = mock_snowflake_details
        mock_credential = MagicMock()
        mock_get_cred.return_value = mock_credential
        mock_adf_client = MagicMock()
        mock_adf_client_class.return_value = mock_adf_client
        mock_create_pipeline.return_value = "test-run-id"

        # Execute
        result = main()

        # Assert
        mock_config.get_azure_details.assert_called_once()
        mock_config.get_snowflake_details.assert_called_once()
        mock_get_cred.assert_called_once()
        mock_adf_client_class.assert_called_once_with(
            mock_credential,
            mock_azure_details["subscription_id"],
        )
        mock_create_df.assert_called_once_with(
            mock_adf_client,
            mock_azure_details["resource_group"],
            mock_azure_details["data_factory_name"],
        )
        mock_create_blob.assert_called_once()
        mock_create_snowflake.assert_called_once()
        mock_create_datasets.assert_called_once()
        mock_create_pipeline.assert_called_once()
        assert result == "test-run-id"
