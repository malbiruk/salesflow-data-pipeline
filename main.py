"""
Entry point script that orchestrates the data pipeline process:
1. Downloads sample data
2. Creates Azure services and uploads to Azure Blob Storage
3. Initializes Snowflake structures
4. Creates and triggers ADF pipeline to load data into Snowflake
"""

from scripts import adf_pipeline_creator, azure_blob_upload, init_snowflake_db
from utils.logger import get_logger


def main() -> None:
    """Execute the full data pipeline initialization sequence."""
    logger = get_logger()

    try:
        logger.info("Starting data pipeline initialization...")

        # Step 1: Upload data to Azure Blob
        logger.info("Uploading data to Azure Blob Storage...")
        azure_blob_upload.main()

        # Step 2: Initialize Snowflake database and schemas
        logger.info("Initializing Snowflake structures...")
        init_snowflake_db.main()

        # Step 3: Create and run ADF pipeline to load data to Snowflake
        logger.info("Creating and running ADF pipeline...")
        adf_pipeline_creator.main()

        logger.info("Pipeline initialization completed successfully!")

    except Exception:
        logger.exception("Pipeline initialization failed")


if __name__ == "__main__":
    main()
