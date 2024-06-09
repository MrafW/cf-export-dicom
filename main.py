import base64
from googleapiclient import discovery
import logging

def dicom_subs(event, context):
    """Background Cloud Function to be triggered by Pub/Sub."""
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        logging.info(f"Received Pub/Sub message: {pubsub_message}")

        dicom_store_path = pubsub_message.strip()
        if not dicom_store_path:
            logging.error("Error: 'DATA' field not found in message.")
            return

        # Extract project ID, location, dataset ID, and DICOM store ID from the path
        parts = dicom_store_path.split('/')
        if len(parts) < 8:
            logging.error("Error: Invalid DICOM store path format in message.")
            return

        project_id = parts[1]
        location = parts[3]
        dataset_id = parts[5]
        dicom_store_id = parts[7]
        uri_prefix = "da-test-dicom"  # Replace with your Cloud Storage bucket name

        # Call the export function
        export_dicom_instance(project_id, location, dataset_id, dicom_store_id, uri_prefix)

    except Exception as e:
        logging.error(f"Error processing Pub/Sub message: {e}")

def export_dicom_instance(project_id, location, dataset_id, dicom_store_id, uri_prefix):
    """Export data to a Google Cloud Storage bucket by copying it from the DICOM store."""
    api_version = "v1"
    service_name = "healthcare"
    client = discovery.build(service_name, api_version)

    dicom_store_parent = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
    dicom_store_name = f"{dicom_store_parent}/dicomStores/{dicom_store_id}"

    body = {"gcsDestination": {"uriPrefix": f"gs://{uri_prefix}"}}

    try:
        request = client.projects().locations().datasets().dicomStores().export(name=dicom_store_name, body=body)
        response = request.execute()
        logging.info(f"Exported DICOM instances to bucket: gs://{uri_prefix}")
        return response
    except Exception as e:
        logging.error(f"Error during DICOM export: {e}")
        raise
