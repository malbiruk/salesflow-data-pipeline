"""
Entry point script that orchestrates the data pipeline process:
1. Downloads sample data
2. Creates Azure services and uploads to Azure Blob Storage
3. Initializes Snowflake structures
4. Creates and triggers ADF pipeline to load data into Snowflake
"""

import shutil
import subprocess
from pathlib import Path

from scripts import adf_pipeline_creator, azure_blob_upload, init_snowflake_db
from utils.logger import get_logger

logger = get_logger()


class PipelineError(Exception):
    """Custom exception for pipeline processing failures."""


def run_dbt() -> bool:
    """Run dbt models to transform data in Snowflake."""
    logger = get_logger()

    try:
        dbt_executable = shutil.which("dbt")
        if not dbt_executable:
            logger.error("dbt executable not found in PATH")
            return False

        dbt_project_path = Path("dbt_salesflow")
        profiles_dir = Path.home() / ".dbt"

        # Run dbt command
        result = subprocess.run(
            [dbt_executable, "run", "--profiles-dir", profiles_dir],
            cwd=dbt_project_path,
            capture_output=True,
            text=True,
            check=False,
        )

        logger.info("dbt stdout: %s", result.stdout)

        if result.returncode != 0:
            logger.error("dbt stderr: %s", result.stderr)
            logger.error("dbt run failed with exit code %d", result.returncode)
            return False

        logger.info("dbt run completed successfully: %s", result.stdout)

        # Optionally run tests
        test_result = subprocess.run(
            [dbt_executable, "test", "--profiles-dir", profiles_dir],
            cwd=dbt_project_path,
            capture_output=True,
            text=True,
            check=False,
        )

        if test_result.returncode != 0:
            logger.error("dbt stderr: %s", test_result.stderr)
            logger.error("dbt run failed with exit code %d", test_result.returncode)
            return False

        logger.info("dbt tests completed: %s", test_result.stdout)

    except subprocess.CalledProcessError:
        logger.exception("dbt command failed")
        return False

    else:
        return True


def main() -> None:
    """Execute the full data pipeline initialization sequence."""

    logger.info("Starting data pipeline initialization...")

    # Step 1: Upload data to Azure Blob
    logger.info("Uploading data to Azure Blob Storage...")
    azure_blob_upload.main()

    # Step 2: Initialize Snowflake database and database_schemas
    logger.info("Initializing Snowflake structures...")
    init_snowflake_db.main()

    # Step 3: Create and run ADF pipeline to load data to Snowflake
    logger.info("Creating and running Azure Data Factory pipeline...")
    adf_pipeline_creator.main()

    # Step 4: Run dbt transformations
    logger.info("Running dbt transformations...")
    if not run_dbt():
        raise PipelineError("dbt processing failed")

    logger.info("Pipeline initialization completed successfully!")


if __name__ == "__main__":
    main()
