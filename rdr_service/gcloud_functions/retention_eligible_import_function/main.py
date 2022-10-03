#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from aou_cloud.services.gcp_cloud_function import GCPCloudFunctionContext, \
    FunctionStoragePubSubHandler
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask


# Function name must contain only lower case Latin letters, digits or underscore. It must
# start with letter, must not end with a hyphen, and must be at most 63 characters long.
# There must be a python function in this file with the same name as the entry point.
function_name = 'retention_eligible_import_function'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-resource=%%CLOUD_RESOURCE%%',
    '--trigger-event google.storage.object.finalize',
    '--timeout=540',
    '--memory=512'
]

task_queue = 'resource-tasks'
_logger = logging.getLogger('function')


class RetentionEligibleImportFunction(FunctionStoragePubSubHandler):

    def created(self):
        """ Handle storage object created event. """
        _logger.info(f"file found: {self.event.name}")

        cloud_file_path = f'{self.event.bucket}/{self.event.name}'

        data = {
            "file_path": cloud_file_path,
            "bucket_name": self.event.bucket,
            "upload_date": self.event.timeCreated,
        }

        # check to see if the file being dropped in the bucket is the retention file before executing the function
        if 'retention' in self.event.name:
            _logger.info("Pushing cloud task...")

            _task = GCPCloudTask()
            _task.execute('/resource/task/ImportRetentionEligibleFileApi', payload=data, queue=task_queue)
        else:
            _logger.info(f"skipping file: {self.event.name}, as it is not the retention file.")


def get_deploy_args(gcp_env):
    """
    Return the trigger and any additional arguments for the 'gcloud functions deploy' command.
    Warning: function get_deploy_args() must come after all class definitions.
    """
    _project_suffix = gcp_env.project.split('-')[-1]

    # Change these to appropriate buckets in derived functions for GCs' buckets
    cloud_resource = 'ptsc-retention-all-of-us-rdr-prod'

    if _project_suffix == 'sandbox':
        cloud_resource = 'ptsc-retention-all-of-us-rdr-sandbox'

    if _project_suffix == 'stable':
        cloud_resource = 'ptsc-metrics-all-of-us-rdr-stable'

    if _project_suffix == 'prod':
        cloud_resource = 'ptsc-metrics-all-of-us-rdr-prod'

    args = [function_name]
    for arg in deploy_args:
        args.append(arg.replace('%%CLOUD_RESOURCE%%', cloud_resource))

    return args


def retention_eligible_import_function(_event, _context):
    """
    GCloud Function Entry Point (Storage Pub/Sub Event).
    https://cloud.google.com/functions/docs/concepts/events-triggers#functions_parameters-python
    :param _event: (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    :param _context: (google.cloud.functions.Context): Metadata of triggering event.
    """
    with GCPCloudFunctionContext(function_name, None) as gcp_env:
        func = RetentionEligibleImportFunction(gcp_env, _event, _context)
        func.run()
