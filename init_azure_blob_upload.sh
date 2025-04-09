#!/bin/bash

source .env

FILE_NAME="10000 Sales Records.csv"
BLOB_NAME="10000 Sales Records.csv"

check_env_vars() {
    if [[ -z "$AZURE_APP_ID" || -z "$AZURE_PASSWORD" || -z "$AZURE_TENANT" || -z "$AZURE_RESOURCE_GROUP" || \
    -z "$AZURE_STORAGE_ACCOUNT" || -z "$AZURE_CONTAINER_NAME" ]]; then
        echo "Error: Missing one or more environment variables."
        exit 1
    fi
}

download_dataset() {
    echo "Downloading dataset..."
    curl -L -o sales-dataset.zip \
    "https://excelbianalytics.com/wp/wp-content/uploads/2017/07/10000-Sales-Records.zip"
    unzip sales-dataset.zip
    rm sales-dataset.zip
}

login_to_azure() {
    echo "Logging in to Azure..."
    az login --service-principal --username "$AZURE_APP_ID" --password "$AZURE_PASSWORD" --tenant "$AZURE_TENANT" \
        || { echo "Azure login failed"; exit 1; }
}

create_azure_resources() {
    echo "Creating Azure resources..."

    az group create --name $AZURE_RESOURCE_GROUP --location uaenorth

    az storage account create \
        --name $AZURE_STORAGE_ACCOUNT \
        --resource-group $AZURE_RESOURCE_GROUP \
        --location eastus \
        --sku Standard_LRS

    STORAGE_KEY=$(az storage account keys list \
        --resource-group $AZURE_RESOURCE_GROUP \
        --account-name $AZURE_STORAGE_ACCOUNT \
        --query '[0].value' -o tsv)

    az storage container create \
        --name $AZURE_CONTAINER_NAME \
        --account-name $AZURE_STORAGE_ACCOUNT \
        --account-key $STORAGE_KEY
}

upload_to_blob() {
    echo "Uploading dataset to Azure Blob..."

    az storage blob upload \
        --account-name $AZURE_STORAGE_ACCOUNT \
        --account-key $STORAGE_KEY \
        --container-name $AZURE_CONTAINER_NAME \
        --name "$BLOB_NAME" \
        --file "$FILE_NAME" \
        --overwrite
}


check_env_vars

download_dataset
login_to_azure
create_azure_resources
upload_to_blob
