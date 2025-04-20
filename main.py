import azure_blob_upload
import init_snowflake_db
from logger import get_logger

logger = get_logger()


def main():
    """Main orchestration function for the data pipeline."""

    azure_blob_upload.main()
    init_snowflake_db.main()

    # Future steps: Add data transformation, loading, etc.
    logger.info("Data pipeline execution completed successfully!")


if __name__ == "__main__":
    main()
