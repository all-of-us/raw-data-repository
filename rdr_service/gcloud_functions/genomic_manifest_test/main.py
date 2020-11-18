#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import csv
import logging
import sys
from urllib.parse import quote

from aou_cloud.services.gcp_cloud_storage import open_cloud_file
from aou_cloud.services.gcp_cloud_function import GCPCloudFunctionContext, FunctionStoragePubSubHandler
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask
from aou_cloud.services.system_utils import JSONObject, setup_logging



# Function name must contain only lower case Latin letters, digits or underscore. It must
# start with letter, must not end with a hyphen, and must be at most 63 characters long.
# There must be a python function in this file with the same name as the entry point.
function_name = 'genomic_manifest_test'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-resource=%%CLOUD_RESOURCE%%',
    '--trigger-event google.storage.object.finalize',
    '--timeout=540',
    '--memory=512'
]

_logger = logging.getLogger('function')


class GenomicManifestTestFunction(FunctionStoragePubSubHandler):

    def created(self):
        """ Handle storage object created event. """
        # Verify this is a file that we want to process.
        if 'aw1_genotyping_sample_manifests' not in self.event.name.lower():
            _logger.info(f'Skipping file {self.event.name}, name does not match AW1 file.')
            return

        _logger.info(f"file found: {self.event.name}")

        cloud_file_path = f'{self.event.bucket}/{self.event.name}'

        data = {
            "filename": cloud_file_path
        }
        
        _task = GCPCloudTask()
        _task.execute('ingest_aw1_manifest_task', payload=data)


def get_deploy_args(gcp_env):
    """
    Return the trigger and any additional arguments for the 'gcloud functions deploy' command.
    Warning: function get_deploy_args() must come after all class definitions.
    """
    _project_suffix = gcp_env.project.split('-')[-1]

    cloud_resource = 'aou-rdr-sandbox-mock-data'

    if _project_suffix == 'sandbox':
        cloud_resource = 'aou-rdr-sandbox-mock-data'

    args = [function_name]
    for arg in deploy_args:
        args.append(arg.replace('%%CLOUD_RESOURCE%%', cloud_resource))

    return args


def genomic_manifest_test(event, context):
    """
    GCloud Function Entry Point (Storage Pub/Sub Event).
    https://cloud.google.com/functions/docs/concepts/events-triggers#functions_parameters-python
    :param event: (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    :param context: (google.cloud.functions.Context): Metadata of triggering event.
    """
    with GCPCloudFunctionContext(function_name, None) as gcp_env:
        func = GenomicManifestTestFunction(gcp_env, event, context)
        func.run()


""" --- Main Program Call --- """
if __name__ == '__main__':
    """ Test code locally """
    setup_logging(_logger, function_name, debug=True)

    #context = FunctionEvent(1669022966780817, 'google.storage.object.finalize')
    file = "AW1_genotyping_sample_manifests/RDR_AoU_GEN_PKG-1908-218052.csv"

    event = {
      "bucket": "aou-rdr-sandbox-mock-data",
      "contentType": "text/csv",
      "crc32c": "jXOx3g==",
      "etag": "CIapmYSb0+wCEAE=",
      "generation": "1603748044887174",
      "id": f"aou-rdr-sandbox-mock-data/{quote(file)}/1603748044887174",
      "kind": "storage#object",
      "md5Hash": "sSsHt9P3l+WieCCevpQsjg==",
      "mediaLink": f"https://www.googleapis.com/download/storage/v1/b/aou-rdr-sandbox-mock-data/o/{quote(file)}?generation=1603748044887174&alt=media",
      "metageneration": "1",
      "name": file,
      "selfLink": f"https://www.googleapis.com/storage/v1/b/aou-rdr-sandbox-mock-data/o/{quote(file)}",
      "size": "1989",
      "storageClass": "STANDARD",
      "timeCreated": "2020-10-26T21:34:04.887Z",
      "timeStorageClassUpdated": "2020-10-26T21:34:04.887Z",
      "updated": "2020-10-26T21:34:04.887Z"
    }

    #sys.exit(genomic_manifest_test(event, context))
