import os, json, logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from requests_toolbelt.multipart.decoder import MultipartDecoder

# Blob client setup
STORAGE_ACCOUNT_NAME = os.getenv("AzureWebJobsStorage__accountName")
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", credential=credential
)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Uploading blob...")

    try:
        # Decode multipart form-data
        content_type = req.headers.get("Content-Type")
        if not content_type:
            return func.HttpResponse("Missing Content-Type", status_code=400)

        body = req.get_body()
        decoder = MultipartDecoder(body, content_type)

        container_name = None
        blob_bytes = None
        blob_filename = None

        for part in decoder.parts:
            content_disposition = part.headers.get(b"Content-Disposition", b"").decode()
            if "name=\"containerName\"" in content_disposition:
                container_name = part.text
            elif "name=\"file\"" in content_disposition:
                blob_bytes = part.content
                # Extract the filename from content-disposition
                if "filename=" in content_disposition:
                    blob_filename = content_disposition.split("filename=")[1].strip("\"' ")

        if not container_name or not blob_bytes or not blob_filename:
            return func.HttpResponse("Missing required fields", status_code=400)

        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_filename
        )
        blob_client.upload_blob(blob_bytes, overwrite=True)

        url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_filename}"
        return func.HttpResponse(
            json.dumps({"message": "Upload successful", "url": url}),
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Upload failed")
        return func.HttpResponse(f"Upload failed: {str(e)}", status_code=500)
