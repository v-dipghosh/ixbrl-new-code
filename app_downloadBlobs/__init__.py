import os, logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# �� Setup Blob Client ��
STORAGE_ACCOUNT_NAME = os.getenv("AzureWebJobsStorage__accountName")
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", credential=credential
)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Downloading blob")
    container = req.params.get("containerName")
    blob_name = req.params.get("blobName")

    if not container or not blob_name:
        return func.HttpResponse("Missing containerName or blobName", status_code=400)

    try:
        client = blob_service_client.get_blob_client(container=container, blob=blob_name)
        downloader = client.download_blob().readall()
        headers = {
            "Content-Disposition": f'attachment; filename="{blob_name}"',
            "Content-Type": "application/octet-stream"
            }

        # headers = {
        #     "Content-Disposition": f"attachment; filename={blob_name}",
        #     "Content-Type": "application/octet-stream"
        # }
        return func.HttpResponse(body=downloader, status_code=200, headers=headers)
    except Exception as e:
        logging.exception("Error downloading blob")
        return func.HttpResponse(f"Download failed: {e}", status_code=500)
