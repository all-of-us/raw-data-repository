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

    def run(self):
        """ Handle Pub/Sub message events.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """
        _logger.info("""This Function was triggered by messageId {} published at {}
            """.format(self.context.event_id, self.context.timestamp))

        _logger.info(f"File found: {self.event.attributes.objectId}")

        object_id = self.event.attributes.objectId.lower()

        short_read_tasks = {
            '_sample_manifests': {
                'manifest_type': 'aw1',
                'task_endpoint': 'IngestAW1ManifestTaskApi'
            },
            'aw1f_pre_results': {
                'manifest_type': 'aw1f',
                'task_endpoint': 'IngestAW1ManifestTaskApi'
            },
            '_data_manifests': {
                'manifest_type': 'aw2',
                'task_endpoint': 'IngestAW2ManifestTaskApi'
            },
            'aw4_': {
                'manifest_type': 'aw4',
                'task_endpoint': 'IngestAW4ManifestTaskApi'
            },
            'aw5_': {
                'manifest_type': 'aw5',
                'task_endpoint': 'IngestAW5ManifestTaskApi'
            },
        }
        gem_tasks = {
            'gem_a2': {
                'manifest_type': 'a2',
                'task_endpoint': 'IngestGemManifestTaskApi'
            }
        }
        cvl_tasks = {
            '_w2sc_': {
                'manifest_type': 'w2sc',
                'task_endpoint': 'IngestCVLManifestTaskApi'
            },
            '_w3ns_': {
                'manifest_type': 'w3ns',
                'task_endpoint': 'IngestCVLManifestTaskApi'
            },
            '_w3sc_': {
                'manifest_type': 'w3sc',
                'task_endpoint': 'IngestCVLManifestTaskApi'
            },
            '_cvl_pkg': {
                'manifest_type': 'w3ss',
                'task_endpoint': 'IngestCVLManifestTaskApi'
            },
            '_w4wr_': {
                'manifest_type': 'w4wr',
                'task_endpoint': 'IngestCVLManifestTaskApi'
            },
            '_w5nf_': {
                'manifest_type': 'w5nf',
                'task_endpoint': 'IngestCVLManifestTaskApi'
            },
        }
        long_read_tasks = {
            '_lr_requests_': {
                'manifest_type': 'lr',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_lr_pkg': {
                'manifest_type': 'l1',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_ont_': {
                'manifest_type': 'l2_ont',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_pbccs_': {
                'manifest_type': 'l2_pb_ccs',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_l4_': {
                'manifest_type': 'l4',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_l5_': {
                'manifest_type': 'l5',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_l6_': {
                'manifest_type': 'l6',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_l1f_': {
                'manifest_type': 'l1f',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_l4f_': {
                'manifest_type': 'l4f',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_l6f_': {
                'manifest_type': 'l6f',
                'task_endpoint': 'IngestSubManifestTaskApi'
            }
        }
        pr_tasks = {
            '_pr_requests_': {
                'manifest_type': 'pr',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_proteomics_pkg': {
                'manifest_type': 'p1',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_p2_': {
                'manifest_type': 'p2',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_p4_': {
                'manifest_type': 'p4',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_p5_': {
                'manifest_type': 'p5',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_p1f_': {
                'manifest_type': 'p1f',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
        }
        rna_tasks = {
           'rr_requests': {
                'manifest_type': 'rr',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_rnaseq_pkg': {
                'manifest_type': 'r1',
                'task_endpoint': 'IngestSubManifestTaskApi'
            },
            '_r2_': {
                'manifest_type': 'r2',
                'task_endpoint': 'IngestSubManifestTaskApi'
            }
        }

        task_key_map = {
            **short_read_tasks,
            **gem_tasks,
            **cvl_tasks,
            **long_read_tasks,
            **pr_tasks,
            **rna_tasks
        }

        for key, value in task_key_map.items():
            if key in object_id:
                task_data: dict = value
                break
        else:
            _logger.info("No files match ingestion criteria.")
            return

        _logger.info(f"Event payload: {self.event}")

        if task_data:
            _logger.info("Pushing cloud tasks...")

            api_route = f'{self.task_root}{task_data.get("task_endpoint")}'
            manifest_type = task_data.get("manifest_type")

            data = {
                "file_type": manifest_type,
                "filename": self.event.attributes.objectId,
                "file_path": f'{self.event.attributes.bucketId}/{self.event.attributes.objectId}',
                "bucket_name": self.event.attributes.bucketId,
                "topic": "genomic_manifest_upload",
                "event_payload": self.event,
                "upload_date": self.event.attributes.eventTime,
                "task": f'{manifest_type}_manifest',
                "api_route": api_route,
                "cloud_function": True,
            }

            # Load into raw table
            raw_cloud_task = GCPCloudTask()
            raw_cloud_task.execute(
                f'{self.task_root}LoadRawAWNManifestDataAPI',
                payload=data,
                queue=task_queue
            )

            cloud_task = GCPCloudTask()
            cloud_task.execute(api_route, payload=data, queue=task_queue)


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
