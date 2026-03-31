import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from config import PROCESSED_CSV_PATH, AZURE_CONTAINER_NAME, BLOB_NAME


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def upload_to_azure_blob(file_path: str, container_name: str, blob_name: str) -> None:
    """
    Uploads a local file to Azure Blob Storage.
    Creates the container automatically if it does not exist.

    Args:
        file_path (str): The local path to the file.
        container_name (str): The name of the Azure container.
        blob_name (str): The destination path and name in the blob container.
    """
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    # Validate connection string
    if not connection_string or connection_string.startswith(
            "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME"):
        logger.error("Invalid or missing Azure Connection String in the .env file.")
        return

    # Validate local file existence
    if not os.path.exists(file_path):
        logger.error(f"The file '{file_path}' does not exist. Please run the data cleaner first.")
        return

    try:
        logger.info("Connecting to Azure Blob Storage...")

        # Initialize the client that connects to Azure
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Connect to the target container
        container_client = blob_service_client.get_container_client(container_name)

        # Check if the container exists; create it if it doesn't
        try:
            container_client.get_container_properties()
        except Exception:
            logger.info(f"Container '{container_name}' not found. Creating it automatically...")
            container_client.create_container()

        # Define the blob client (the destination file inside the cloud)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        logger.info(f"Uploading data to Azure as '{blob_name}'...")

        # Open the local CSV file and stream the upload
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logger.info("SUCCESS! Data is now safely stored in the cloud.")

    except Exception as e:
        logger.error(f"Failed to upload to Azure Blob Storage: {e}")


def main() -> None:
    logger.info("Starting the upload process...")
    upload_to_azure_blob(
        file_path=PROCESSED_CSV_PATH,
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_NAME
    )


if __name__ == "__main__":
    main()