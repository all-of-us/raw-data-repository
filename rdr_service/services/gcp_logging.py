# pylint: disable=unused-import
import collections
from datetime import datetime, timezone
import os
import requests
import json

from google.cloud import logging
from google.cloud.logging.resource import Resource
# https://github.com/googleapis/google-cloud-python/issues/2572
from google.protobuf import json_format, any_pb2

from rdr_service.services import gcp_request_log_pb2
from google.logging.type import http_request_pb2

# https://cloud.google.com/appengine/docs/standard/python3/writing-application-logs
# https://googleapis.dev/python/logging/latest/usage.html
# https://any-api.com/googleapis_com/logging/docs/Definitions/LogEntry
# https://cloud.google.com/appengine/docs/standard/python3/runtime
# https://cloud.google.com/logging/docs/api/v2/resource-list
# https://github.com/googleapis/googleapis/blob/master/google/appengine/logging/v1/request_log.proto
# https://developers.google.com/protocol-buffers/docs/downloads


def setup_logging_zone():
    """
    Attempt to get the project zone information.
    return: zone string.
    """
    zone = 'local-machine'
    if 'GAE_SERVICE' in os.environ:
        try:
            resp = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/zone', timeout=15.0)
            if resp.status_code == 200:
                zone = resp.text.strip()
        # pylint: disable=broad-except
        except Exception:
            zone = 'unknown'
    return zone


logging_zone = setup_logging_zone()


def setup_logging_resource():
    """
    Set the values for the Google Logging Resource object
    :return: Resource object
    """
    labels = {
        "project_id": os.environ.get('GAE_APPLICATION', 'localhost'),
        "module_id": os.environ.get('GAE_SERVICE', 'default'),
        "version_id": os.environ.get('GAE_VERSION', 'develop'),
        "zone": logging_zone
    }

    resource = Resource(type='gae_app', labels=labels)
    return resource


def setup_log_line(message: str, event_ts: str, level: str):
    """
    Prepare a log event for sending to GCP StackDriver.
    :param message: Log message.
    :param event_ts: Log event timestamp.
    :param level: Log severity level.
    :return: LogLine proto buffer object
    """
    line = {
        "logMessage": message if message else '',
        "severity": level if level else 'INFO',
        "sourceLocation": {
            "file": "/base/data/home/apps/s~all-of-us-rdr-stable/v1-55-rc2.421152339935578377/app_util.py",
            "functionName": "request_logging",
            "line": 183
        },
        "time": event_ts
    }

    return line


def setup_request_log(lines: list):

    req_dict = {
        "@type": "type.googleapis.com/google.appengine.logging.v1.RequestLog",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "method": "GET",
        "status": 418,
        "latency": "2.11s",
        "ip": "127.0.0.1",
        "first": True,
        "finished": True,
        "end_time": datetime.now(timezone.utc).isoformat(),
        "resource": "/Bad-Mojo-Teacups/Logging",
        "responseSize": 355,
        "userAgent": "Bad Mojo",
        "line": []
    }

    if lines and len(lines) > 0:
        for x in range(len(lines)):
            req_dict["line"].append(lines[x])

    # req = gcp_request_log_pb2.RequestLog()
    # req = json_format.Parse(json.dumps(req_dict), req)

    # Convert dict to Generic pb2 message object.
    msg = json_format.ParseDict(req_dict, any_pb2.Any())

    return msg


def log_event_to_stackdriver():

    lines = []
    lines.append(setup_log_line("This is a Teacup message for Michael.",
                                    datetime.now(timezone.utc).isoformat(), "INFO"))
    lines.append(
        setup_log_line("Bad Mojo Teacups Rock!", datetime.now(timezone.utc).isoformat(), "ERROR"))

    pb = setup_request_log(lines)

    log = {
        "message": "This is a Teacup message for Michael.",
        "httpRequest": {"status": 513},
        "severity": "INFO",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # z = json_format.MessageToJson(pb)


    client = logging.Client(project='all-of-us-rdr-sandbox')
    logger = client.logger('appengine.googleapis.com%2Frequest_log')

    # https://google-cloud-python.readthedocs.io/en/0.32.0/logging/logger.html
    # {"message": "This is a Teacup message for Michael.", "httpRequest": {"status": 200}, "severity": "INFO"}
    logger.log_proto(message=pb, severity="ERROR", resource=setup_logging_resource())

    return log


class GCPStackDriverLogHandler(logging.Handler):
    """
    Sends log records to google stackdriver logging.
    Buffers up to `buffer_size` log records into one protobuffer to be submitted.
    """

    def __init__(self, trace_id, buffer_size=10):
        self._trace_id = trace_id
        self._buffer_size = buffer_size
        self._buffer = collections.deque()
        client = logging.Client(project='all-of-us-rdr-sandbox')  #TODO: pass in other envs.
        self.logger = client.logger('appengine.googleapis.com%2Frequest_log')

    def emit(self, record):
        self._buffer.append(self.format(record))
        if len(self._buffer) >= self._buffer_size:
            self.publish_to_stackdriver()

    def publish_to_stackdriver(self):
        lines = list(map(setup_log_line, self._buffer))
        if lines:
            self.logger.log_proto(
                message=setup_request_log(lines),
                severity=self.get_highest_severity_level_from_lines(lines),
                resource=setup_logging_resource(),
                trace=self._trace_id
            ) 

    @staticmethod
    def get_highest_severity_level_from_lines(lines):
        if lines:
            return sorted(
                [line['severity'] for line in lines],
                key=lambda severity: -getattr(logging, severity, 0)
            )[0]
        else:
            return None


class FlaskGCPStackDriverLoggingMiddleware:
    """
    Adds a special log handler for each request.
    Ensures that protobuffers contain only messages from one request.
    """

    def __init__(self, app):
        self.root_logger = logging.getLogger()
        self.app = app

    def __call__(self, environ, start_response):
        """
        NOTE: The `X-Cloud-Trace-Context` header is only available in GAE Python 2.7.
            In Python 3 the recommended setup is: https://cloud.google.com/trace/docs/setup/python
        """
        trace_id = 0  # see note in docstring
        handler = GCPStackDriverLogHandler(trace_id)
        self.root_logger.addHandler(handler)
        try:
            return self.app(environ, start_response)
        except:
            raise
        finally:
            self.root_logger.removeHandler(handler)
