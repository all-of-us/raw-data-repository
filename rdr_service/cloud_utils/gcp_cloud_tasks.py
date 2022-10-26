from datetime import datetime, timedelta, date
import json
import logging
from time import sleep

from google.api_core.exceptions import InternalServerError, GoogleAPICallError
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

from rdr_service.config import GAE_PROJECT
from rdr_service.services.flask import TASK_PREFIX


class GCPCloudTask(object):
    """
    Use the GCP Cloud Tasks API to run a task later.
    """
    # Create a client.
    _client = None

    def execute(self, endpoint: str, payload: (dict, list) = None, in_seconds: int = 0, project_id: str = GAE_PROJECT,
                location: str = 'us-central1', queue: str = 'default', quiet=False):
        """
        Make GCP Cloud Task API request to run task later.
        :param endpoint: Flask API endpoint to call.
        :param payload: dict containing data to send to task.
        :param in_seconds: delay before starting task in seconds, default to run immediately.
        :param project_id: target project id.
        :param location: target location.
        :param queue: target cloud task queue.
        :param quiet: suppress logging.
        """
        if not project_id or project_id == 'localhost':
            raise ValueError('Invalid GCP project id')
        if not self._client:
            self._client = tasks_v2.CloudTasksClient()

        if not endpoint:
            raise ValueError('endpoint value must be provided.')
        if payload and not isinstance(payload, dict):
            raise TypeError('payload must be a dict object.')

        from rdr_service.resource.main import app
        if endpoint not in app.url_map._rules_by_endpoint:
            raise ValueError('endpoint is not registered in app.')
        res = app.url_map._rules_by_endpoint[endpoint][0]

        if not res.rule.startswith(TASK_PREFIX):
            raise ValueError('endpoint is not configured using the task prefix.')

        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return obj.__repr__()

        if payload:
            payload = json.dumps(payload, default=json_serial).encode()

        # Construct the fully qualified queue name.
        parent = self._client.queue_path(project_id, location, queue)

        task = {
            "app_engine_http_request": {
                "http_method": "POST",
                "relative_uri": res.rule
            }
        }

        if payload:
            task['app_engine_http_request']['body'] = payload

        if in_seconds:
            run_ts = datetime.utcnow() + timedelta(seconds=in_seconds)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(run_ts)
            task['schedule_time'] = timestamp

        # Use the client to build and send the task.
        retry = 5
        while retry:
            retry -= 1
            try:
                response = self._client.create_task(parent=parent, task=task)
                if not quiet:
                    logging.info('Created task {0}'.format(response.name))
                return
            except (InternalServerError, GoogleAPICallError):
                sleep(0.25)
        logging.error('Create Cloud Task Failed.')
