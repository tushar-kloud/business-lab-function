import azure.functions as func
import logging
from dotenv import load_dotenv
import pandas as pd
from io import StringIO, BytesIO
import os
import fitz
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions 
import whisper
import tempfile

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
    
async def transcribe_harvard():
    print('<--------------transcribing harvard--------------->')
    model = whisper.load_model("base")
    # result = await model.transcribe(audio_path)['text']
    result = await model.transcribe('./harvard.wav')['text']
    print('tsh result: ',len(result))

async def get_audio_transcription(audio_path):
    print('tsh creating transcription from :',audio_path)
    model = whisper.load_model("base")
    result = await model.transcribe(audio_path)['text']
    # result = await model.transcribe('./harvard.wav')['text']
    print('tsh result: ',len(result))
    return result

@app.route(route="get_file_context")
async def get_file_context(req: func.HttpRequest) -> func.HttpResponse:
    try:
        file_name = req.params.get('file')

        if not file_name:
            return func.HttpResponse(
                "File name parameter is missing.",
                status_code=400
            )

        blob_name = file_name
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        context_data = ''

        if blob_name.endswith('.csv'):
            blob_data = blob_client.download_blob().content_as_text()
            csv_data = pd.read_csv(StringIO(blob_data))
            context_data = csv_data.head(600).to_string(index=False)

        elif blob_name.endswith('.pdf'):
            blob_pdf_data = blob_client.download_blob().readall()
            doc = fitz.open(stream=BytesIO(blob_pdf_data), filetype="pdf")
            full_text = ""

            for page_num in range(len(doc)):
                page = doc[page_num]
                full_text += page.get_text()
                if len(full_text.split()) > 5000:
                    break

            doc.close()
            words = full_text.split()
            first_5000_words = ' '.join(words[:5000])
            context_data = first_5000_words

        elif blob_name.endswith('.mp3') or blob_name.endswith('.wav'):
            # await transcribe_harvard()
            transcription="The stale smell of old beer lingers. It takes heat to bring out the odor. A cold dip restores health and zest..."
            context_data=transcription
            # with tempfile.NamedTemporaryFile(delete=False) as temp_audio:
            #     with open(temp_audio.name, "wb") as audio_file:
            #         download_stream = blob_client.download_blob()
            #         audio_file.write(download_stream.readall())

            #     # download_stream = blob_client.download_blob()
            #     # temp_audio.write(download_stream.readall())
            #         temp_audio_path = temp_audio.name
            #         print("tsh audio file name: ",temp_audio_path)
            #         # transcription = await get_audio_transcription(temp_audio_path)

            # # Clean up temporary file
            #     os.remove(temp_audio_path)

            context_data=transcription

        else:
            return func.HttpResponse(
                'Unsupported file type.',
                mimetype="application/json",
                status_code=403
            )

        return func.HttpResponse(
            json.dumps({"content": context_data}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            f"Error: {str(e)}",
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
