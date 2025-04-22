from azure.identity import ClientSecretCredential

from utils import config
from utils.logger import get_logger

logger = get_logger()


def get_azure_credential() -> ClientSecretCredential:
    """Get Azure credential using service principal."""
    logger.info("Authenticating with Azure...")
    azure_credentials = config.get_azure_details()
    return ClientSecretCredential(**azure_credentials)
