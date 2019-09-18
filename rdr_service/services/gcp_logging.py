import os
import requests

from google.cloud import logging
from google.cloud.logging.resource import Resource



# https://cloud.google.com/appengine/docs/standard/python3/runtime


def setup_logging_resource():
    """
    Set the values for the Google Logging Resource object
    :return: Resource object
    """
    zone = 'local-machine'
    try:
        resp = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/zone', timeout=10.0)
        if resp.status_code == 200:
            zone = resp.text.strip()
    except Exception:
        pass

    labels = {
        "project_id": os.environ.get('GAE_APPLICATION', 'localhost'),
        "module_id": os.environ.get('GAE_SERVICE', 'default'),
        "version_id": os.environ.get('GAE_VERSION', 'develop'),
        "zone": zone
    }

    resource = Resource(type='gae_app', labels=labels)
    return resource

_res = setup_logging_resource()

#
# logger = c.logger('appengine.googleapis.com%2Frequest_log')
#
# c = logging.Client(project='all-of-us-rdr-sandbox')

# logger.log_text('{ "message": "Michael", "weather": "partly cloudy"}', resource=res, severity="INFO", httpRequest={"status": 500"}, protoPayload='{"@type": "type.googleapis.com/google.appengine.logging.v1.Req
# uestLog", "line": [{"0": {"test": "test line 0"}}, {"1": {"t
# est": "test line 1"}}], "method":"POST"}', resource="/offline/Michael")