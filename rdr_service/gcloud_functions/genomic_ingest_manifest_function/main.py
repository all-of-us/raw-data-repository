#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import sys

from aou_cloud.services.gcp_cloud_function import GCPCloudFunctionContext, \
    PubSubEventContext, FunctionPubSubHandler
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask
from aou_cloud.services.system_utils import setup_logging

# Function name must contain only lower case Latin letters, digits or underscore. It must
# start with letter, must not end with a hyphen, and must be at most 63 characters long.
# There must be a python function in this file with the same name as the entry point.
function_name = 'genomic_ingest_manifest_function'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-topic genomic_manifest_upload',
    '--timeout=540',
    '--memory=512'
]

task_queue = 'genomics'
_logger = logging.getLogger('function')


class GenomicIngestManifestFunction(FunctionPubSubHandler):

    def __init__(self, gcp_env, _event, _context):
        super().__init__(gcp_env, _event, _context)

        self.task_root = '/resource/task/'

        self.task_mappings = {
            "aw1": "IngestAW1ManifestTaskApi",
            "aw1f": "IngestAW1ManifestTaskApi",
            "aw2": "IngestAW2ManifestTaskApi",
            "aw4": "IngestAW4ManifestTaskApi",
            "aw5": "IngestAW5ManifestTaskApi",
            "w2sc": "IngestCVLManifestTaskApi",
            "w3ns": "IngestCVLManifestTaskApi",
            "w3sc": "IngestCVLManifestTaskApi",
            "w3ss": "IngestCVLManifestTaskApi",
            "w4wr": "IngestCVLManifestTaskApi",
            "w5nf": "IngestCVLManifestTaskApi",
        }

    def run(self):
        """ Handle Pub/Sub message events.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """

        _logger.info("""This Function was triggered by messageId {} published at {}
            """.format(self.context.event_id, self.context.timestamp))

        _logger.info(f"File found: {self.event.attributes.objectId}")

        object_id = self.event.attributes.objectId.lower()

        # AW1 files have "_sample_manifests" in file name
        if '_sample_manifests' in object_id:
            task_key = "aw1"

        # AW1F files have "aw1f_pre_results" in file name
        elif 'aw1f_pre_results' in object_id:
            task_key = "aw1f"

        # AW2 files have "_data_manifests" in their file name
        elif '_data_manifests' in object_id:
            task_key = "aw2"

        # AW4 files have "AW4" in their file path (bucket name)
        elif 'aw4_' in object_id:
            task_key = "aw4"

        # AW5 files have "AW5" in their file path (bucket name)
        elif 'aw5_' in object_id:
            task_key = "aw5"

        # W2SC files have "W2SC" in their file path (bucket name)
        elif '_w2sc_' in object_id:
            task_key = "w2sc"

        # W3NS files have "W3NS" in their file path (bucket name)
        elif '_w3ns_' in object_id:
            task_key = "w3ns"

        # W3SC files have "W3SC" in their file path (bucket name)
        elif '_w3sc_' in object_id:
            task_key = "w3sc"

        # W3SS files have "_PKG" in their file path (bucket name)
        elif '_pkg' in object_id:
            task_key = "w3ss"

        # W4WR files have "W4WR" in their file path (bucket name)
        elif '_w4wr_' in object_id:
            task_key = "w4wr"

        # W5NF files have "W5NF" in their file path (bucket name)
        elif '_w5nf_' in object_id:
            task_key = "w5nf"

        else:
            _logger.info("No files match ingestion criteria.")
            return

        _logger.info(f"Event payload: {self.event}")

        if task_key:
            _logger.info("Pushing cloud tasks...")

            api_route = f'{self.task_root}{self.task_mappings[task_key]}'

            data = {
                "file_type": task_key,
                "filename": self.event.attributes.objectId,
                "file_path": f'{self.event.attributes.bucketId}/{self.event.attributes.objectId}',
                "bucket_name": self.event.attributes.bucketId,
                "topic": "genomic_manifest_upload",
                "event_payload": self.event,
                "upload_date": self.event.attributes.eventTime,
                "task": f'{task_key}_manifest',
                "api_route": api_route,
                "cloud_function": True,
            }

            raw_manifest_keys = ['aw1', 'aw2', 'aw4', 'w2sc', 'w3ns', 'w3sc', 'w3ss', 'w4wr', 'w5nf']

            # Load into raw table
            if task_key in raw_manifest_keys:
                _task = GCPCloudTask()
                _task.execute(
                    f'{self.task_root}LoadRawAWNManifestDataAPI',
                    payload=data,
                    queue=task_queue
                )

            _task = GCPCloudTask()
            _task.execute(api_route, payload=data, queue=task_queue)


def get_deploy_args(gcp_env):
    """
    Return the trigger and any additional arguments for the 'gcloud functions deploy' command.
    Warning: function get_deploy_args() must come after all class definitions.
    """
    _project_suffix = gcp_env.project.split('-')[-1]

    # Customize args here
    if _project_suffix == "sandbox":
        pass

    args = [function_name]
    for arg in deploy_args:
        args.append(arg)

    return args


def genomic_ingest_manifest_function(_event, _context):
    """ Background Cloud Function to be triggered by Pub/Sub.
    event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    https://cloud.google.com/functions/docs/calling/pubsub#sample_code
    :param _event: (dict): The dictionary with data specific to this type of event.
                          The `data` field contains the PubsubMessage message.
                          The `attributes` field will contain custom attributes if there are any.
                          format described here: https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    :param _context: (google.cloud.functions.Context): Metadata of triggering event.
    """
    with GCPCloudFunctionContext(function_name, None) as gcp_env:
        func = GenomicIngestManifestFunction(gcp_env, _event, _context)
        func.run()


""" --- Main Program Call --- """
if __name__ == '__main__':
    """ Test code locally """
    setup_logging(_logger, function_name, debug=True)

    context = PubSubEventContext(1669022966780817, 'google.pubsub.v1.PubsubMessage')
    file = "AW1_genotyping_sample_manifests/RDR_AoU_GEN_PKG-1908-218052.csv"

    event = {
        "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
        "attributes": {
            "bucketId": "aou-rdr-sandbox-mock-data",
            "eventTime": "2021-04-19T16:02:41.919922Z",
            "eventType": "OBJECT_FINALIZE",
            "notificationConfig": "projects/_/buckets/aou-rdr-sandbox-mock-data/notificationConfigs/34",
            "objectGeneration": "1618848161894414",
            "objectId": "AW1_genotyping_sample_manifests/RDR_AoU_GEN_PKG-1908-218054.csv",
            "overwroteGeneration": "1618605912794149",
            "payloadFormat": "JSON_API_V1"
        }
    }

    sys.exit(genomic_ingest_manifest_function(event, context))
