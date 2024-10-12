import os
import json
import requests
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import DefaultAzureCredential
from datetime import datetime

# Mapping data types to destinations
DESTINATION_MAP = {
    "TypeA": "https://destinationA.com/api",
    "TypeB": "https://destinationB.com/api",
    "TypeC": "https://destinationC.com/api",
    # Add more mappings as needed
}

# Azure Key Vault details
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL")
STORAGE_ACCOUNT_NAME_SECRET = "StorageAccountName"
STORAGE_ACCOUNT_KEY_SECRET = "StorageAccountKey"
LOG_CONTAINER_NAME = os.getenv("LOG_CONTAINER_NAME")

# Initialize the Key Vault client with DefaultAzureCredential (supports Managed Identity)
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# Retrieve secrets from Key Vault using Managed Identity
STORAGE_ACCOUNT_NAME = secret_client.get_secret(STORAGE_ACCOUNT_NAME_SECRET).value
STORAGE_ACCOUNT_KEY = secret_client.get_secret(STORAGE_ACCOUNT_KEY_SECRET).value

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient(account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", credential=credential)
log_blob_client = blob_service_client.get_blob_client(container=LOG_CONTAINER_NAME, blob="processing_logs.json")

def get_destination(data_type):
    return DESTINATION_MAP.get(data_type)

def log_status(blob_name, status, error_message=None):
    log_entry = {
        "blob_name": blob_name,
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "error_message": error_message
    }

    try:
        # Download the existing log file if it exists
        existing_logs = json.loads(log_blob_client.download_blob().readall().decode())
    except Exception:
        # If the log file doesn't exist, start with an empty list
        existing_logs = []

    # Append the new log entry
    existing_logs.append(log_entry)

    # Upload the updated log file
    log_blob_client.upload_blob(json.dumps(existing_logs), overwrite=True)

def process_blob(blob_name, data_type):
    try:
        # Get the BlobClient
        blob_client = blob_service_client.get_blob_client(container=os.getenv("CONTAINER_NAME"), blob=blob_name)

        # Download the blob data
        download_stream = blob_client.download_blob()
        data = download_stream.readall()

        # Determine the destination
        destination = get_destination(data_type)
        if not destination:
            raise ValueError(f"No destination found for data type '{data_type}'")

        # Send data to the destination
        response = requests.post(destination, data=data)

        if response.status_code == 200:
            print(f"Successfully sent blob '{blob_name}' to {destination}.")
            log_status(blob_name, "Success")
        else:
            print(f"Failed to send blob '{blob_name}' to {destination}. Status code: {response.status_code}")
            log_status(blob_name, "Failed", f"Status code: {response.status_code}")

    except Exception as e:
        print(f"Error processing blob '{blob_name}': {str(e)}")
        log_status(blob_name, "Error", str(e))

def main(event: EventGridEvent):
    # Extract blob name and data type from the event
    blob_name = event.data['url'].split('/')[-1]
    data_type = event.data['metadata']['dataType']  # Assuming data type is included in metadata

    # Process the blob
    process_blob(blob_name, data_type)
