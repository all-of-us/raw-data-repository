import json
import logging
import os

from cloudevents.http import CloudEvent
import functions_framework
from google.cloud import tasks_v2

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — override via environment variables set on the function
# ---------------------------------------------------------------------------
GCP_PROJECT = os.environ.get("GCP_PROJECT", "all-of-us-rdr-stable")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
TASK_QUEUE = os.environ.get("TASK_QUEUE", "nph")
API_ROUTE = "/resource/task/NphSmsIngestionTaskApi"


def _enqueue_task(client: tasks_v2.CloudTasksClient, api_route: str, payload: dict) -> None:
    """Create a single App Engine Cloud Task targeting the given relative route."""
    parent = client.queue_path(GCP_PROJECT, GCP_LOCATION, TASK_QUEUE)
    task = tasks_v2.Task(
        app_engine_http_request=tasks_v2.AppEngineHttpRequest(
            http_method=tasks_v2.HttpMethod.POST,
            relative_uri=api_route,
            headers={"Content-Type": "application/json"},
            body=json.dumps(payload).encode(),
        )
    )
    response = client.create_task(
        tasks_v2.CreateTaskRequest(parent=parent, task=task))
    _logger.info("Created task: %s", response.name)


@functions_framework.cloud_event
def nph_sms_manifest_ingestion_function_v2(cloud_event: CloudEvent) -> None:
    """Gen2 Cloud Run Function triggered by a Pub/Sub topic.

    Routes GCS object-created events to the NphSmsIngestionTaskApi Cloud Task
    based on the object path. Skips archived files and unrecognized paths.
    """
    _logger.info("Event ID: %s  |  Event time: %s",
                 cloud_event["id"], cloud_event["time"])

    message = cloud_event.data.get("message", {})
    attributes = message.get("attributes", {})

    bucket_name = attributes.get("bucketId", "")
    object_id = attributes.get("objectId", "")

    if not object_id:
        _logger.warning(
            "No objectId in Pub/Sub message attributes — nothing to do.")
        return

    _logger.info("File detected: gs://%s/%s", bucket_name, object_id)

    object_id_lower = object_id.lower()

    if "archive" in object_id_lower:
        _logger.info("%s is archived, skipping ingestion.", object_id)
        return

    if "pull_lists" in object_id_lower:
        file_type = "SAMPLE_LIST"
    elif "n0_manifest" in object_id_lower:
        file_type = "N0"
    else:
        _logger.info("%s not configured for ingestion.", object_id)
        return

    payload = {
        "file_path":      f"{bucket_name}/{object_id}",
        "bucket_name":    bucket_name,
        "topic":          "sms_files_upload",
        "job":            "FILE_INGESTION",
        "file_type":      file_type,
        "api_route":      API_ROUTE,
        "cloud_function": True,
    }

    _logger.info("Enqueueing Cloud Task for file_type=%s", file_type)
    client = tasks_v2.CloudTasksClient()
    _enqueue_task(client, API_ROUTE, payload)
    _logger.info("Done.")
