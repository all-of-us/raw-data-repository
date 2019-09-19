import os
import requests

from google.cloud import logging
from google.cloud.logging.resource import Resource
# https://github.com/googleapis/google-cloud-python/issues/2572
from rdr_service.services import gcp_request_log_pb2
from google.logging.type import http_request_pb2

# https://googleapis.dev/python/logging/latest/usage.html
# https://any-api.com/googleapis_com/logging/docs/Definitions/LogEntry
# https://cloud.google.com/appengine/docs/standard/python3/runtime
# https://cloud.google.com/logging/docs/api/v2/resource-list
# https://github.com/googleapis/googleapis/blob/master/google/appengine/logging/v1/request_log.proto
# https://developers.google.com/protocol-buffers/docs/downloads

def setup_logging_resource():
    """
    Set the values for the Google Logging Resource object
    :return: Resource object
    """
    zone = 'local-machine'
    if 'GAE_SERVICE' in os.environ:
        try:
            resp = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/zone', timeout=15.0)
            if resp.status_code == 200:
                zone = resp.text.strip()
        except Exception:
            zone = 'unknown'

    labels = {
        "project_id": os.environ.get('GAE_APPLICATION', 'localhost'),
        "module_id": os.environ.get('GAE_SERVICE', 'default'),
        "version_id": os.environ.get('GAE_VERSION', 'develop'),
        "zone": zone
    }

    resource = Resource(type='gae_app', labels=labels)
    return resource

_res = setup_logging_resource()

req = gcp_request_log_pb2.RequestLog()



#
# logger = c.logger('appengine.googleapis.com%2Frequest_log')
#
# c = logging.Client(project='all-of-us-rdr-sandbox')

# logger.log_text('{ "message": "Michael", "weather": "partly cloudy"}', resource=res, severity="INFO", httpRequest={"status": 500"}, protoPayload='{"@type": "type.googleapis.com/google.appengine.logging.v1.Req
# uestLog", "line": [{"0": {"test": "test line 0"}}, {"1": {"t
# est": "test line 1"}}], "method":"POST"}', resource="/offline/Michael")