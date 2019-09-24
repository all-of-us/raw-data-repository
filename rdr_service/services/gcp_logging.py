# pylint: disable=unused-import
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




# req = gcp_request_log_pb2.RequestLog()
# json_format.MessageToJson(req)
#


#
# logger = c.logger('appengine.googleapis.com%2Frequest_log')
#
# c = logging.Client(project='all-of-us-rdr-sandbox')

