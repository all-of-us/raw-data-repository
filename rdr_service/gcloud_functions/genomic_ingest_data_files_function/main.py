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
function_name = 'genomic_ingest_data_files_function'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-topic genomic_data_files_upload',
    '--timeout=540',
    '--memory=512'
]

task_queue = 'genomics'
_logger = logging.getLogger('function')


class GenomicIngestFilesFunction(FunctionPubSubHandler):

    def __init__(self, gcp_env, _event, _context):
        super().__init__(gcp_env, _event, _context)
        self.task_route = '/resource/task/IngestDataFilesTaskApi'

    def run(self):
        """ Handle Pub/Sub message events.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """

        _logger.info("""This Function was triggered by messageId {} published at {}
            """.format(self.context.event_id, self.context.timestamp))

        _logger.info(f"Event payload: {self.event}")

        proceed_to_task = False
        allowed_files = ['hard-filtered.gvcf.gz', 'hard-filtered.gvcf.gz.md5sum']

        file_path = self.event.attributes.objectId.replace(" ", "")
        ext = file_path.split('.', 1)[-1]

        if ext in allowed_files:
            proceed_to_task = True

        # prod buckets for gvcf files / Only attaching gvcf notifications for now
        # prod-genomics-data-baylor/Wgs_sample_raw_data/SS_VCF_research/
        # prod-genomics-data-baylor/Wgs_sample_raw_data/SS_VCF_research/
        # prod-genomics-data-northwest/wgs_sample_raw_data/ss_vcf_research/

        data = {
            "file_path": file_path,
            "bucket_name": self.event.attributes.bucketId,
        }

        _logger.info("Pushing cloud tasks...")

        if proceed_to_task:
            _task = GCPCloudTask()
            _task.execute(f'{self.task_route}', payload=data, queue=task_queue)
        else:
            _logger.info("File processed from Event payload not allowed")

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


def genomic_ingest_data_files_function(_event, _context):
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
        func = GenomicIngestFilesFunction(gcp_env, _event, _context)
        func.run()


""" --- Main Program Call --- """
if __name__ == '__main__':
    """ Test code locally """
    setup_logging(_logger, function_name, debug=True)

    context = PubSubEventContext(1620919933899502, 'google.pubsub.v1.PubsubMessage')
    file = "Wgs_sample_raw_data/SS_VCF_research/BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz.md5sum"

    event = {
        "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
        "attributes": {
            "bucketId": "aou-rdr-sandbox-mock-data",
            "eventTime": "2021-05-13T15:32:13.910124Z",
            "eventType": "OBJECT_FINALIZE",
            "notificationConfig": "projects/_/buckets/aou-rdr-sandbox-mock-data/notificationConfigs/58",
            "objectGeneration": "1620919933899502",
            "objectId": "Wgs_sample_raw_data/ SS_VCF_research/BCM_A100153482_21042005280_SIA0013441__1.hard-filtered"
                        ".gvcf.gz.md5sum",
            "overwroteGeneration": "1620919548016598",
            "payloadFormat": "JSON_API_V1"
        }
    }

    sys.exit(genomic_ingest_data_files_function(event, context))
