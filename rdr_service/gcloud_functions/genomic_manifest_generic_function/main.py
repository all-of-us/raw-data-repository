#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import base64
import logging
import sys

from aou_cloud.services.gcp_cloud_function import GCPCloudFunctionContext, \
    PubSubEventContext, FunctionPubSubHandler
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask
from aou_cloud.services.system_utils import setup_logging

# Function name must contain only lower case Latin letters, digits or underscore. It must
# start with letter, must not end with a hyphen, and must be at most 63 characters long.
# There must be a python function in this file with the same name as the entry point.
function_name = 'genomic_manifest_generic_function'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-topic aw1_ingestion_test',
    '--timeout=540',
    '--memory=512'
]

task_queue = 'genomics'
_logger = logging.getLogger('function')


class GenomicManifestGenericFunction(FunctionPubSubHandler):

    def run(self):
        """ Handle Pub/Sub message events.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """

        _logger.info("""This Function was triggered by messageId {} published at {}
            """.format(self.context.event_id, self.context.timestamp))

        # Verify this is a file that we want to process.
        if 'aw1_genotyping_sample_manifests' not in self.event.attributes.objectId.lower():
            return

        _logger.info(f"file found: {self.event.attributes.objectId}")

        cloud_file_path = f'{self.event.attributes.bucketId}/{self.event.attributes.objectId}'

        data = {
            "file_path": cloud_file_path,
            "bucket_name": self.event.attributes.bucketId,
            "upload_date": self.event.attributes.eventTime,
        }

        _logger.info("Pushing cloud task...")

        _task = GCPCloudTask()
        _task.execute('/resource/task/IngestAW1ManifestTaskApi', payload=data, queue=task_queue)


def get_deploy_args(gcp_env):
    """
    Return the trigger and any additional arguments for the 'gcloud functions deploy' command.
    Warning: function get_deploy_args() must come after all class definitions.
    """
    _project_suffix = gcp_env.project.split('-')[-1]

    # Customize args here

    args = [function_name]
    for arg in deploy_args:
        args.append(arg)

    return args


def genomic_manifest_generic_function(_event, _context):
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
        func = GenomicManifestGenericFunction(gcp_env, _event, _context)
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

    sys.exit(genomic_manifest_generic_function(event, context))
