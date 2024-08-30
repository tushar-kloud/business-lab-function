import azure.functions as func
import logging
from dotenv import load_dotenv
import pandas as pd
from io import StringIO
import PyPDF2
import os
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions 

# Load environment variables from .env file
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Environment variable for Blob Storage connection string
BLOB_STORAGE_CONNECTION_STRING = os.environ.get('BLOB_STORAGE_CONNECTION_STRING')

@app.route(route="bizz_lab_func")
def bizz_lab_func(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function that returns a personalized greeting if 'name' parameter is provided.

    Query Parameter:
    - name (string): Name of the person to greet.

    Request Body:
    - JSON with 'name' field (optional).

    Responses:ba
    - 200 OK: Personalized greeting message if 'name' is provided.
    - 200 OK: Default message if 'name' is not provided.

    Example Request:
    GET /api/bizz_lab_func?name=John
    POST /api/bizz_lab_func with body {"name": "John"}

    Example Response:
    - "Hello, John. This HTTP triggered function executed successfully."
    - "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
    """
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

container_name = 'business-labs'

@app.route(route="get_files_list")
def get_files_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function to list all files in the specified Azure Blob Storage container.

    Responses:
    - 200 OK: JSON array of file names in the container.
    - 500 Internal Server Error: Error message if there is an issue retrieving the file list.

    Example Request:
    GET /api/get_files_list

    Example Response:
    - ["file1.csv", "file2.pdf"]
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()

        files_list = [items.name for items in blob_list]

        return func.HttpResponse(
            json.dumps(files_list, indent=4, sort_keys=True, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"Error: {e}",
            status_code=500
        )

@app.route(route="get_file_context")
def get_file_context(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function to retrieve and process the content of a specified file from Azure Blob Storage.

    Query Parameter:
    - file (string): Name of the file to retrieve.

    Supported File Types:
    - CSV: Returns the first 10 rows of the CSV file.

    Responses:
    - 200 OK: JSON formatted content of the file.
    - 403 Forbidden: Unsupported file type.
    - 500 Internal Server Error: Error message if there is an issue processing the file.

    Example Request:
    GET /api/get_file_context?file=file1.csv

    Example Response:
    - JSON content of the file (for CSV files).
    """
    try:
        file_name = req.params.get('file')

        blob_name = file_name
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().content_as_text()

        context_data = ''

        if blob_name.endswith('.csv'):
            # Handle CSV file
            csv_data = pd.read_csv(StringIO(blob_data))
            context_data = csv_data.head(10).to_string(index=False)
        else:
            return func.HttpResponse(
                f'Do not have support for this file type yet',
                mimetype="application/json",
                status_code=403
            )
        
        return func.HttpResponse(
            json.dumps(
                context_data,
                indent=4,
                sort_keys=True,
                default=str
            ),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"Error: {e}",
            status_code=500
        )

@app.route(route='trigger-api')
def test_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function for testing the API endpoint.

    Responses:
    - 200 OK: Confirmation message indicating that the API was triggered successfully.

    Example Request:
    GET /api/trigger-api

    Example Response:
    - "API Triggered successfully!"
    """
    return func.HttpResponse(
        "API Triggered successfully!"
    )