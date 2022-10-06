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
function_name = 'genomic_ingest_metrics_function'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-topic genomic_metric_files_upload',
    '--timeout=540',
    '--memory=512'
]

task_queue = 'genomics'
_logger = logging.getLogger('function')


class GenomicIngestMetricsFunction(FunctionPubSubHandler):

    def __init__(self, gcp_env, _event, _context):
        super().__init__(gcp_env, _event, _context)

        self.task_root = '/resource/task/'

        self.task_mappings = {
            "user_metrics": "IngestUserEventMetricsApi",
            "appointment_metrics": "IngestAppointmentMetricsApi"
        }

    def run(self):
        """ Handle Pub/Sub message events.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """
        _logger.info("""This Function was triggered by messageId {} published at {}
            """.format(self.context.event_id, self.context.timestamp))

        task_key = None
        object_id = self.event.attributes.objectId.lower()

        for metric_str_key, api_route in self.task_mappings.items():
            if metric_str_key in object_id:
                task_key = metric_str_key
                break

        if not task_key:
            _logger.info(f"No files match ingestion criteria. {object_id}")
            return

        _logger.info(f"Event payload: {self.event}")

        if task_key:
            _logger.info("Pushing cloud tasks...")

            api_route = f'{self.task_root}{self.task_mappings[task_key]}'

            data = {
                "file_path": f'{self.event.attributes.bucketId}/{self.event.attributes.objectId}',
                "bucket_name": self.event.attributes.objectId,
                "topic": "genomic_metric_files_upload",
                "event_payload": self.event,
                "task": "metrics_ingest",
                "api_route": api_route,
                "cloud_function": True,
            }

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


def genomic_ingest_metrics_function(_event, _context):
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
        func = GenomicIngestMetricsFunction(gcp_env, _event, _context)
        func.run()

""" --- Main Program Call --- """
if __name__ == '__main__':
    """ Test code locally """
    setup_logging(_logger, function_name, debug=True)

    context = PubSubEventContext(1620919933899502, 'google.pubsub.v1.PubsubMessage')
    file = "user_events_two/ghr3_user_events.csv"

    event = {
        "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
        "attributes": {
            "bucketId": "aou-rdr-sandbox-mock-data",
            "eventTime": "2021-12-21T17:30:59.761138Z",
            "eventType": "OBJECT_FINALIZE",
            "notificationConfig": "projects/_/buckets/aou-rdr-sandbox-mock-data/notificationConfigs/94",
            "objectGeneration": "1640107859749184",
            "objectId": "user_events_two/ghr3_user_events.csv",
            "payloadFormat": "JSON_API_V1"}
    }

    sys.exit(genomic_ingest_metrics_function(event, context))
