from unittest.mock import MagicMock, mock_open, patch

import pytest
import snowflake.connector

from scripts.init_snowflake_db import SCHEMA_RAW, execute_sql_statements, main


@pytest.fixture
def mock_snowflake_credentials():
    """Fixture for Snowflake credentials."""
    return {
        "account": "test-account",
        "user": "test-user",
        "password": "test-password",
        "database": "TEST_DB",
        "warehouse": "TEST_WH",
    }


@pytest.fixture
def mock_cursor():
    """Fixture for Snowflake cursor."""
    cursor = MagicMock(spec=snowflake.connector.cursor.SnowflakeCursor)
    cursor.execute = MagicMock()
    return cursor


@pytest.fixture
def mock_connection():
    """Fixture for Snowflake connection."""
    return MagicMock(spec=snowflake.connector.SnowflakeConnection)


class TestExecuteSqlStatements:
    """Tests for the execute_sql_statements function."""

    def test_execute_sql_statements_success(self, mock_cursor):
        """Test successful SQL statement execution."""
        # Setup
        sql_script = "CREATE TABLE test_table (id INT);\nCREATE TABLE another_table (name VARCHAR);"

        # Execute
        execute_sql_statements(mock_cursor, sql_script)

        # Assert
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_any_call("CREATE TABLE test_table (id INT)")
        mock_cursor.execute.assert_any_call("CREATE TABLE another_table (name VARCHAR)")

    def test_execute_sql_statements_with_empty_statements(self, mock_cursor):
        """Test SQL execution with empty statements that should be skipped."""
        # Setup
        sql_script = (
            "CREATE TABLE test_table (id INT);;;\nCREATE TABLE another_table (name VARCHAR);"
        )

        # Execute
        execute_sql_statements(mock_cursor, sql_script)

        # Assert
        assert mock_cursor.execute.call_count == 2

    def test_execute_sql_statements_error_handling(self, mock_cursor):
        """Test error handling during SQL execution."""
        # Setup
        sql_script = "CREATE TABLE test_table (id INT);\nINVALID SQL STATEMENT;"
        mock_cursor.execute.side_effect = [
            None,  # First call succeeds
            snowflake.connector.errors.ProgrammingError("Invalid SQL"),  # Second call fails
        ]

        # Execute
        execute_sql_statements(mock_cursor, sql_script)

        # Assert - should continue despite the error
        assert mock_cursor.execute.call_count == 2


class TestMainFunction:
    """Tests for the main function."""

    @patch("scripts.init_snowflake_db.config.get_snowflake_details")
    @patch("scripts.init_snowflake_db.snowflake.connector.connect")
    @patch("scripts.init_snowflake_db.Path")
    def test_main_function_flow(
        self,
        mock_path,
        mock_connect,
        mock_get_snowflake_details,
        mock_snowflake_credentials,
        mock_cursor,
        mock_connection,
    ):
        """Test the main function's overall flow."""
        # Setup
        mock_get_snowflake_details.return_value = mock_snowflake_credentials
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the SQL file opening and reading
        mock_sql_content = "CREATE TABLE RAW_SALES_DATA (ID INT, DATA VARCHAR);"
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = mock_sql_content
        mock_path_instance = MagicMock()
        mock_path_instance.open.return_value = mock_file
        mock_path.return_value = mock_path_instance

        # Execute
        main()

        # Assert
        mock_get_snowflake_details.assert_called_once()
        mock_connect.assert_called_once_with(**mock_snowflake_credentials)
        mock_connection.cursor.assert_called_once()

        # Check database and schema creation
        mock_cursor.execute.assert_any_call(
            f"CREATE DATABASE IF NOT EXISTS {mock_snowflake_credentials['database']}",
        )
        mock_cursor.execute.assert_any_call(
            f"USE DATABASE {mock_snowflake_credentials['database']}",
        )
        mock_cursor.execute.assert_any_call(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")
        mock_cursor.execute.assert_any_call(f"USE SCHEMA {SCHEMA_RAW}")

        # Verify SQL file was processed
        mock_path.assert_called_once_with("db_schema/raw_schema.sql")
        mock_path_instance.open.assert_called_once()

        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("scripts.init_snowflake_db.config.get_snowflake_details")
    @patch("scripts.init_snowflake_db.snowflake.connector.connect")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="CREATE TABLE RAW_SALES_DATA (ID INT);",
    )
    def test_main_with_actual_file_mocking(
        self,
        mock_file_open,
        mock_connect,
        mock_get_snowflake_details,
        mock_snowflake_credentials,
        mock_cursor,
        mock_connection,
    ):
        """Test main function with a different approach to mocking the file."""
        # Setup
        mock_get_snowflake_details.return_value = mock_snowflake_credentials
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Execute
        with patch("pathlib.Path.open", mock_file_open):
            main()

        # Assert
        mock_cursor.execute.assert_any_call("CREATE TABLE RAW_SALES_DATA (ID INT)")

    @patch("scripts.init_snowflake_db.config.get_snowflake_details")
    @patch("scripts.init_snowflake_db.snowflake.connector.connect")
    def test_connection_error_handling(
        self,
        mock_connect,
        mock_get_snowflake_details,
        mock_snowflake_credentials,
    ):
        """Test error handling when connection fails."""
        # Setup
        mock_get_snowflake_details.return_value = mock_snowflake_credentials
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Connection failed")

        # Execute & Assert
        with pytest.raises(snowflake.connector.errors.DatabaseError):
            main()
