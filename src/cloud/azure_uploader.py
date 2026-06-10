import os
import sys
import logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from config import (
    AZURE_BRONZE_CONTAINER_NAME, BLOB_NAME, RAW_JSON_PATH, AZURE_SILVER_CONTAINER_NAME,
    AZURE_GOLD_CONTAINER_NAME, OUTPUT_DIR_SILVER, OUTPUT_DIR_GOLD
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


def upload_file_to_azure_blob(file_path: str, container_name: str, blob_name: str) -> None:
    """
    Uploads a local file to Azure Blob Storage.
    Creates the container automatically if it does not exist.

    Args:
        file_path (str): The local path to the file.
        container_name (str): The name of the Azure container.
        blob_name (str): The destination path and name in the blob container.
    """

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

        # Open the local file in binary mode and stream the upload
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logger.info("SUCCESS! Data is now safely stored in the cloud.")

    except Exception as e:
        logger.error(f"Failed to upload to Azure Blob Storage: {e}")


def upload_folder_to_azure(local_folder: str, container_name: str) -> None:
    """
    Uploads all files from a local folder to Azure Blob Storage.
    Preserves the folder structure as blob paths inside the container.

    Args:
        local_folder (str): The local folder path to upload recursively.
        container_name (str): The name of the Azure container.
    """
    if not connection_string or connection_string.startswith(
            "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME"):
        logger.error("Invalid or missing Azure Connection String in the .env file.")
        return

    if not os.path.exists(local_folder):
        logger.error(f"The folder '{local_folder}' does not exist.")
        return

    try:
        logger.info(f"Connecting to Azure Blob Storage (container: '{container_name}')...")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        try:
            container_client.get_container_properties()
        except Exception:
            logger.info(f"Container '{container_name}' not found. Creating it automatically...")
            container_client.create_container()

        uploaded = 0

        for root, dirs, files in os.walk(local_folder):
            for file in files:
                local_path = os.path.join(root, file)
                blob_name = os.path.relpath(local_path, local_folder)

                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

                with open(local_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

                logger.info(f"  Uploaded: {blob_name}")
                uploaded += 1

        logger.info(f"SUCCESS! {uploaded} files uploaded to container '{container_name}'.")

    except Exception as e:
        logger.error(f"Failed to upload folder to Azure Blob Storage: {e}")



def main() -> None:
    logger.info("Starting the upload process...")
    upload_file_to_azure_blob(
        file_path=RAW_JSON_PATH,
        container_name=AZURE_BRONZE_CONTAINER_NAME,
        blob_name=BLOB_NAME
    )

    upload_folder_to_azure(
        local_folder=OUTPUT_DIR_SILVER,
        container_name=AZURE_SILVER_CONTAINER_NAME
    )

    upload_folder_to_azure(
        local_folder=OUTPUT_DIR_GOLD,
        container_name=AZURE_GOLD_CONTAINER_NAME
    )


if __name__ == "__main__":
    main()