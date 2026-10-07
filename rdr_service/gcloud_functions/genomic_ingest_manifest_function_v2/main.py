import json
import logging
import os

from cloudevents.http import CloudEvent
import functions_framework
from google.cloud import tasks_v2

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# Configuration — override via environment variables set on the function
GCP_PROJECT = os.environ.get("GCP_PROJECT", "all-of-us-rdr-stable")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
TASK_QUEUE = os.environ.get("TASK_QUEUE", "genomics")
TASK_ROOT = "/resource/task/"


# Manifest-type routing table  (key = substring matched against object path)
TASK_KEY_MAP = {
    # Short-read
    "_sample_manifests":  {"manifest_type": "aw1",  "task_endpoint": "IngestAW1ManifestTaskApi"},
    "aw1f_pre_results":   {"manifest_type": "aw1f", "task_endpoint": "IngestAW1ManifestTaskApi"},
    "_data_manifests":    {"manifest_type": "aw2",  "task_endpoint": "IngestAW2ManifestTaskApi"},
    "aw4_":               {"manifest_type": "aw4",  "task_endpoint": "IngestAW4ManifestTaskApi"},
    "aw5_":               {"manifest_type": "aw5",  "task_endpoint": "IngestAW5ManifestTaskApi"},
    # GEM
    "gem_a2":             {"manifest_type": "a2",   "task_endpoint": "IngestGemManifestTaskApi"},
    # CVL
    "_w2sc_":             {"manifest_type": "w2sc", "task_endpoint": "IngestCVLManifestTaskApi"},
    "_w3ns_":             {"manifest_type": "w3ns", "task_endpoint": "IngestCVLManifestTaskApi"},
    "_w3sc_":             {"manifest_type": "w3sc", "task_endpoint": "IngestCVLManifestTaskApi"},
    "_cvl_pkg":           {"manifest_type": "w3ss", "task_endpoint": "IngestCVLManifestTaskApi"},
    "_w4wr_":             {"manifest_type": "w4wr", "task_endpoint": "IngestCVLManifestTaskApi"},
    "_w5nf_":             {"manifest_type": "w5nf", "task_endpoint": "IngestCVLManifestTaskApi"},
    # Long-read
    "_lr_requests_":      {"manifest_type": "lr",      "task_endpoint": "IngestSubManifestTaskApi"},
    "_lr_pkg":            {"manifest_type": "l1",      "task_endpoint": "IngestSubManifestTaskApi"},
    "_ont_":              {"manifest_type": "l2_ont",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_pbccs_":            {"manifest_type": "l2_pb_ccs", "task_endpoint": "IngestSubManifestTaskApi"},
    "_l4_":               {"manifest_type": "l4",      "task_endpoint": "IngestSubManifestTaskApi"},
    "_l5_":               {"manifest_type": "l5",      "task_endpoint": "IngestSubManifestTaskApi"},
    "_l6_":               {"manifest_type": "l6",      "task_endpoint": "IngestSubManifestTaskApi"},
    "_l1f_":              {"manifest_type": "l1f",     "task_endpoint": "IngestSubManifestTaskApi"},
    "_l4f_":              {"manifest_type": "l4f",     "task_endpoint": "IngestSubManifestTaskApi"},
    "_l6f_":              {"manifest_type": "l6f",     "task_endpoint": "IngestSubManifestTaskApi"},
    # Proteomics
    "_pr_requests_":      {"manifest_type": "pr",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_proteomics_pkg":    {"manifest_type": "p1",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_p2_":               {"manifest_type": "p2",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_p4_":               {"manifest_type": "p4",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_p5_":               {"manifest_type": "p5",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_p1f_":              {"manifest_type": "p1f", "task_endpoint": "IngestSubManifestTaskApi"},
    # RNA
    "rr_requests":        {"manifest_type": "rr",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_rnaseq_pkg":        {"manifest_type": "r1",  "task_endpoint": "IngestSubManifestTaskApi"},
    "_r2_":               {"manifest_type": "r2",  "task_endpoint": "IngestSubManifestTaskApi"},
}


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


# Entry point — triggered by a Pub/Sub CloudEvent (Eventarc)
@functions_framework.cloud_event
def genomic_ingest_manifest_function_v2(cloud_event: CloudEvent) -> None:
    """Gen2 Cloud Run Function triggered by a Pub/Sub topic.

    The Pub/Sub message carries a GCS OBJECT_FINALIZE notification in its
    attributes (bucketId, objectId, eventTime).  We route by object path,
    then enqueue two Cloud Tasks: one to load the raw manifest data, and one
    for the type-specific ingest endpoint.
    """
    _logger.info("Event ID: %s  |  Event time: %s",
                 cloud_event["id"], cloud_event["time"])

    #
    # Extract GCS metadata from the Pub/Sub message attributes.
    # For a GCS notification routed through Pub/Sub the attributes live
    # directly on cloud_event.data["message"]["attributes"].
    #
    message = cloud_event.data.get("message", {})
    attributes = message.get("attributes", {})

    bucket_name = attributes.get("bucketId", "")
    object_id = attributes.get("objectId", "")
    event_time = attributes.get("eventTime", "")

    if not object_id:
        _logger.warning(
            "No objectId in Pub/Sub message attributes — nothing to do.")
        return

    _logger.info("File detected: gs://%s/%s", bucket_name, object_id)

    # Route by filename substring
    object_id_lower = object_id.lower()
    task_data = None
    for key, value in TASK_KEY_MAP.items():
        if key in object_id_lower:
            task_data = value
            break

    if task_data is None:
        _logger.info(
            "Object path does not match any ingestion criteria — skipping.")
        return

    manifest_type = task_data["manifest_type"]
    task_endpoint = task_data["task_endpoint"]
    api_route = f"{TASK_ROOT}{task_endpoint}"

    payload = {
        "file_type":      manifest_type,
        "filename":       object_id,
        "file_path":      f"{bucket_name}/{object_id}",
        "bucket_name":    bucket_name,
        "topic":          "genomic_manifest_upload",
        "upload_date":    event_time,
        "task":           f"{manifest_type}_manifest",
        "api_route":      api_route,
        "cloud_function": True,
    }

    _logger.info("Enqueueing Cloud Tasks for manifest_type=%s", manifest_type)
    client = tasks_v2.CloudTasksClient()

    # 1. Load into raw table
    _enqueue_task(client, f"{TASK_ROOT}LoadRawAWNManifestDataAPI", payload)
    # 2. Type-specific ingest
    _enqueue_task(client, api_route, payload)

    _logger.info("Done.")
