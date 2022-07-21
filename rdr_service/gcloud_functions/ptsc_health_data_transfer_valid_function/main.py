#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from aou_cloud.services.gcp_cloud_function import GCPCloudFunctionContext, FunctionPubSubHandler
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask

# Function name must contain only lower case Latin letters, digits or underscore. It must
# start with letter, must not end with a hyphen, and must be at most 63 characters long.
# There must be a python function in this file with the same name as the entry point.
function_name = 'ptsc_health_data_transfer_valid_function'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.
deploy_args = [
    '--trigger-topic ptsc_health_data_transfer',
    '--timeout=540',
    '--memory=512'
]

task_queue = 'resource-tasks'
_logger = logging.getLogger('function')


class PtscHealthDataTransferValidFunction(FunctionPubSubHandler):

    def __init__(self, gcp_env, _event, _context):
        super().__init__(gcp_env, _event, _context)
        self.task_route = '/resource/task/PtscHealthDataTransferValidTaskApi'

    def run(self):
        """ Handle Pub/Sub message events.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """

        _logger.info("""This Function was triggered by messageId {} published at {}
            """.format(self.context.event_id, self.context.timestamp))

        _logger.info(f"Event payload: {self.event}")

        _task = GCPCloudTask()
        _task.execute(self.task_route, payload=self.event.to_dict(), queue=task_queue)


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


def ptsc_health_data_transfer_valid_function(_event, _context):
    with GCPCloudFunctionContext(function_name, None) as gcp_env:
        func = PtscHealthDataTransferValidFunction(gcp_env, _event, _context)
        func.run()
