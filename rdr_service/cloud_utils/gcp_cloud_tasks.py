from datetime import datetime, timedelta
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
    _project_id = None
    _location = None
    _queue = None
    _payload = None
    _in_seconds = 0
    _uri = None

    def __init__(self, endpoint: str, payload: (dict, list)=None, in_seconds: int = 0, project_id: str = None,
                        location: str = 'us-central1', queue: str = 'default'):
        """
        Initialize a GCP Cloud Task call.
        :param endpoint: Flask API endpoint to call.
        :param payload: dict containing data to send to task.
        :param in_seconds: delay before starting task in seconds, default to run immediately.
        :param project_id: target project id.
        :param location: target location.
        :param queue: target cloud task queue.
        """
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
        self._uri = res.rule

        if project_id:
            self._project_id = project_id
        else:
            self._project_id = GAE_PROJECT

        self._location = location
        self._queue = queue
        if payload:
            self._payload = json.dumps(payload).encode()
        self._in_seconds = in_seconds

    def execute(self, quiet=False):
        """
        Make GCP Cloud Task API request to run task later.
        """
        # Create a client.
        client = tasks_v2.CloudTasksClient()
        # Construct the fully qualified queue name.
        parent = client.queue_path(self._project_id, self._location, self._queue)

        task = {
            "app_engine_http_request": {
                "http_method": "POST",
                "relative_uri": self._uri
            }
        }

        if self._payload:
            task['app_engine_http_request']['body'] = self._payload

        if self._in_seconds:
            run_ts = datetime.utcnow() + timedelta(seconds=self._in_seconds)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(run_ts)
            task['schedule_time'] = timestamp

        # Use the client to build and send the task.
        retry = 5
        while retry:
            retry -= 1
            try:
                response = client.create_task(parent, task)
                if not quiet:
                    logging.info('Created task {0}'.format(response.name))
                return
            except (InternalServerError, GoogleAPICallError):
                sleep(0.25)
        logging.error('Create Cloud Task Failed.')

