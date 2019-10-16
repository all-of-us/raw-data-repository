import collections
import logging
import os
import random
import string
from datetime import datetime, timezone
from enum import IntEnum

import requests
from flask import request

from google.api.monitored_resource_pb2 import MonitoredResource
from google.cloud import logging as gcp_logging
from google.cloud import logging_v2 as gcp_logging_v2
from google.logging.type import http_request_pb2 as gcp_http_request_pb2
from google.protobuf import json_format as gcp_json_format, any_pb2 as gcp_any_pb2

# pylint: disable=unused-import
from rdr_service.services import gcp_request_log_pb2
from rdr_service.config import GAE_PROJECT

# https://pypi.org/project/google-cloud-logging/
# https://cloud.google.com/logging/docs/reference/v2/rpc/google.logging.v2
# https://developers.google.com/resources/api-libraries/documentation/logging/v2/python/latest/logging_v2.entries.html

# How many log lines should be batched before pushing them to StackDriver.
_LOG_BUFFER_SIZE = 10


class LogCompletionStatusEnum(IntEnum):
    """
    Indicator for log entry completion status, which can span multiple log entries.
    """
    COMPLETE = 0
    PARTIAL_BEGIN = 1
    PARTIAL_MORE = 2
    PARTIAL_FINISHED = 3


def setup_logging_zone():
    """
    Attempt to get the project zone information.
    return: Zone pb2 structure.
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


logging_zone_pb2 = setup_logging_zone()


def setup_logging_resource():
    """
    Set the values for the Google Logging Resource object
    :return: MonitoredResource pb2 structure.
    """
    labels = {
        "project_id": GAE_PROJECT,
        "module_id": os.environ.get('GAE_SERVICE', 'default'),
        "version_id": os.environ.get('GAE_VERSION', 'develop'),
        "zone": logging_zone_pb2
    }

    # https://cloud.google.com/logging/docs/reference/v2/rpc/google.api#google.api.MonitoredResource
    resource_pb2 = MonitoredResource(type='gae_app', labels=labels)
    return resource_pb2


logging_resource_pb2 = setup_logging_resource()


def setup_log_line(record: logging.LogRecord):
    """
    Prepare a log event for sending to GCP StackDriver.
    :param record: Log event record.
    :param message: Log message.
    :param event_ts: Log event timestamp.
    :param level: Log severity level.
    :return: LogLine proto buffer object
    """
    event_ts = datetime.utcfromtimestamp(record.created)
    event_ts = event_ts.replace(tzinfo=timezone.utc)
    event_ts = event_ts.isoformat()
    severity = gcp_logging._helpers._normalize_severity(record.levelno)
    message = record.msg if record.msg else ''
    line = {
        "logMessage": message,
        "severity": severity,
        "sourceLocation": {
            "file": "/base/data/home/apps/s~all-of-us-rdr-stable/v1-55-rc2.421152339935578377/app_util.py",
            "functionName": "request_logging",
            "line": 183
        },
        "time": event_ts
    }

    return line


def setup_proto_payload(lines: list, log_status: LogCompletionStatusEnum, **kwargs):
    """
    Build the log protoPayload portion of the log entry.
    :param lines: List of LogMessage lines to add.
    :param log_status: Logging completion status value.
    :return: RequestLog pb2 object.
    """

    # Base set of values for proto_payload object.
    req_dict = {
        "@type": "type.googleapis.com/google.appengine.logging.v1.RequestLog",
        "startTime": datetime.now(timezone.utc).isoformat(),
        "ip": "0.0.0.0",
        "first": True,
        "finished": True,
        "endTime": datetime.now(timezone.utc).isoformat(),
        "responseSize": 355,
        "line": [],
        # If we see these lines in the logs, we know something isn't working correctly.
        "userAgent": "Bad Mojo",
        "resource": "/Bad-Mojo-Teacups/Logging",
    }

    # Override any key values.
    for k, v in kwargs.items():
        req_dict[k] = v

    # Set completion statuses
    if log_status == LogCompletionStatusEnum.PARTIAL_BEGIN:
        req_dict['finished'] = False
    elif log_status == LogCompletionStatusEnum.PARTIAL_MORE:
        req_dict['first'] = False
        req_dict['finished'] = False
    elif log_status == LogCompletionStatusEnum.PARTIAL_FINISHED:
        req_dict['first'] = False

    if lines and len(lines) > 0:
        for x in range(len(lines)):
            req_dict["line"].append(lines[x])

    # Convert dict to Generic pb2 message object, requires gcp_request_log_pb2 import.
    request_log_pb2 = gcp_json_format.ParseDict(req_dict, gcp_any_pb2.Any())

    return request_log_pb2


class GCPStackDriverLogHandler(logging.Handler):
    """
    Sends log records to google stackdriver logging.
    Buffers up to `buffer_size` log records into one protobuffer to be submitted.
    """

    # Used to determine how long a request took.
    __first_log_ts = None

    def __init__(self, buffer_size=_LOG_BUFFER_SIZE):

        super(GCPStackDriverLogHandler, self).__init__()
        self._buffer_size = buffer_size
        self._buffer = collections.deque()

        self._reset()

        self._logging_client = gcp_logging_v2.LoggingServiceV2Client()

        self._request_id = None
        self._operation_pb2 = None

    def _reset(self):

        self.__first_log_ts = None

        self.log_completion_status = LogCompletionStatusEnum.COMPLETE
        self._operation_pb2 = None
        self._request_id = None

        self._trace_id = None
        self._start_time = None
        self._end_time = None

        self._request_method = None
        self._request_endpoint = None
        self._request_resource = None
        self._request_agent = None
        self._request_ip_addr = None

        self._response_status_code = None
        self._response_size = None

        self._buffer.clear()

    def _update_long_operation(self, op_status):
        """
        Handle long operations.
        :param op_status: LogCompletionStatusEnum value.
        """
        if not self._request_id:
            self._request_id = ''.join(random.choice('abcdef' + string.digits) for _ in range(114))

        first = True if op_status == LogCompletionStatusEnum.PARTIAL_BEGIN else False
        last = True if op_status == LogCompletionStatusEnum.PARTIAL_FINISHED else False

        # https://cloud.google.com/logging/docs/reference/v2/rpc/google.logging.v2#google.logging.v2.LogEntryOperation
        self._operation_pb2 = gcp_logging_v2.proto.log_entry_pb2.LogEntryOperation(
            id=self._request_id,
            producer='all-of-us.raw-data-repository/rdr-service',
            first=first,
            last=last
        )

    def setup_from_request(self, req):
        """
        Gather everything we need to log from the request object.
        :param req: Flask request object
        """
        # send any pending log entries in-case 'end_request' was not called.
        if len(self._buffer):
            self.finalize()

        self._start_time = datetime.now(timezone.utc).isoformat()
        self._trace_id = req.headers.get('X-Cloud-Trace-Context', '')
        self._request_method = req.method
        self._request_endpoint = req.endpoint
        self._request_resource = req.path
        self._request_agent = str(req.user_agent)
        self._request_ip_addr = req.remote_addr


    def emit(self, record: logging.LogRecord):
        """
        Capture and store a log event record.
        :param record: Python log record
        """
        self._buffer.append(record)

        if not self.__first_log_ts:
            self.__first_log_ts = datetime.utcnow()

        if len(self._buffer) >= self._buffer_size:
            if self.log_completion_status == LogCompletionStatusEnum.COMPLETE:
                self.log_completion_status = LogCompletionStatusEnum.PARTIAL_BEGIN
                self._update_long_operation(self.log_completion_status)

            elif self.log_completion_status == LogCompletionStatusEnum.PARTIAL_BEGIN:
                self.log_completion_status = LogCompletionStatusEnum.PARTIAL_MORE
                self._update_long_operation(self.log_completion_status)

            self.publish_to_stackdriver()

    def finalize(self, response=None):
        """
        Finalize and send any log entries to StackDriver.
        """
        if response:
            self._response_status_code = response.status_code
            self._response_size = len(response.data)

        if len(self._buffer):
            if self.log_completion_status != LogCompletionStatusEnum.COMPLETE:
                self.log_completion_status = LogCompletionStatusEnum.PARTIAL_FINISHED
                self._update_long_operation(self.log_completion_status)

            self.publish_to_stackdriver()

        self._reset()


    def publish_to_stackdriver(self):
        """
        Send a set of log entries to StackDriver.
        """
        lines = list(map(setup_log_line, self._buffer))
        self._end_time = datetime.now(timezone.utc).isoformat()


        log_entry_pb2_args = {
            'resource': logging_resource_pb2,
            'severity': self.get_highest_severity_level_from_lines(lines),
            'trace': self._trace_id
        }

        if self._response_status_code:
            log_entry_pb2_args['http_request'] = gcp_http_request_pb2.HttpRequest(status=self._response_status_code)
            if self._response_status_code > int(log_entry_pb2_args['severity']):
                log_entry_pb2_args['severity'] = gcp_logging_v2.gapic.enums.LogSeverity(self._response_status_code)

        if self._operation_pb2:
            log_entry_pb2_args['operation'] = self._operation_pb2

        proto_payload_args = {
            'startTime': self._start_time,
            'endTime': self._end_time,
            'method': self._request_method,
            'resource': self._request_resource,
            'userAgent': self._request_agent,
            'ip': self._request_ip_addr,
            'responseSize': self._response_size,
            'status': self._response_status_code,
            'requestId': self._request_id
        }

        if self.__first_log_ts:
            total_time = datetime.utcnow() - self.__first_log_ts
            proto_payload_args['latency'] = '{0}.{1}s'.format(total_time.seconds, total_time.microseconds)

        proto_payload_pb2 = setup_proto_payload(
            lines,
            self.log_completion_status,
            **proto_payload_args
        )

        log_entry_pb2_args['proto_payload'] = proto_payload_pb2

        # https://cloud.google.com/logging/docs/reference/v2/rpc/google.logging.v2#google.logging.v2.LogEntry
        log_entry_pb2 = gcp_logging_v2.types.log_entry_pb2.LogEntry(**log_entry_pb2_args)

        self._logging_client.write_log_entries([log_entry_pb2],
            log_name="projects/{project_id}/logs/appengine.googleapis.com%2Frequest_log".
                                    format(project_id=GAE_PROJECT))

        # remove any log entries from buffer.
        self._buffer.clear()

    @staticmethod
    def get_highest_severity_level_from_lines(lines):
        """
        Figure out the highest severity level in a given set of log records.
        :param lines: List of log records
        """
        if lines:
            s = sorted(
                [line['severity'] for line in lines],
                key=lambda severity: -getattr(logging, "severity", 0)
            )
            return s[0]
        else:
            return None


class FlaskGCPStackDriverLogging:
    """
    Context Manager to handle logging to GCP StackDriver logging service.
    """

    _log_handler = None

    def __init__(self, log_level=logging.INFO):
        if 'GAE_SERVICE' in os.environ:
            # Configure root logger
            self.root_logger = logging.getLogger()
            self.root_logger.setLevel(log_level)
            # Configure StackDriver logging handler
            self._log_handler = GCPStackDriverLogHandler()
            self._log_handler.setLevel(log_level)
            # Add StackDriver logging handler to root logger.
            self.root_logger.addHandler(self._log_handler)

    def begin_request(self):
        """
        Initialize logging for a new request.
        """
        if self._log_handler:
            self._log_handler.setup_from_request(request)

    def end_request(self, response):
        """
        Finalize and send any log entries.  Not guarantied to always be called.
        """
        if self._log_handler:
            self._log_handler.finalize(response)

        return response

    def flush(self):
        """
        Flush any pending log records.
        """
        if self._log_handler:
            logging.info('End of request transaction.')
            self._log_handler.finalize()
