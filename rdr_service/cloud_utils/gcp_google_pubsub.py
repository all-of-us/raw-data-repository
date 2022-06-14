#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Manage google PubSub services
# https://cloud.google.com/pubsub/docs/reference/rest
#
from datetime import datetime
import base64
import json
import logging

from apiclient import errors
from googleapiclient import discovery

from rdr_service.config import GAE_PROJECT
from rdr_service.services.system_utils import retry_func


_INSTANCE_MAPPING = {
    'all-of-us-rdr-prod': 'all-of-us-rdr-prod:us-central1:rdrbackupdb-b',
    'all-of-us-rdr-stable': 'all-of-us-rdr-stable:us-central1:rdrbackupdb',
    'pmi-drc-api-test': 'pmi-drc-api-test:us-central1:rdrmaindb',
    'all-of-us-rdr-sandbox': 'all-of-us-rdr-sandbox:us-central1:rdrmaindb'
}

_ALLOWED_PROJECTS = ['pmi-drc-api-test', 'all-of-us-rdr-stable', 'all-of-us-rdr-sandbox']


class GCPGooglePubSubTopic:
    """ Represents a Google PubSub topic """

    service = None
    project = None
    topic = None
    topic_path = None

    def __init__(self, project_id: str, topic: str):
        """
        :param project_id: Google project id to use
        :param topic: Pub-sub topic id
        """
        self.service = discovery.build('pubsub', 'v1', cache_discovery=False)
        self.project = project_id
        self.topic = topic
        self.topic_path = f'projects/{self.project}/topics/{self.topic}'

    def publish(self, message: dict):
        """
        Publish a pub-sub message to an existing topic.
        https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
        :param message: Data to publish to pub-sub topic.
        :return: operation result
        """
        payload = json.dumps(message).encode('utf-8')
        body = {
            "messages": [
                { 'data': base64.b64encode(payload).decode('utf-8') }
            ]
        }

        req = self.service.projects().topics().publish(topic=self.topic_path, body=body)
        resp = req.execute()
        return resp


def publish_pdr_pubsub(table: str, action: str, pk_column: str, ids: (list, tuple)):
    """
    Publish database table updates to the 'data-pipeline' pub-sub topic.
    :param table: Table name
    :param action: must be one of 'insert', 'update', 'delete' or 'upsert'.
    :param pk_column: Name of primary key column in table
    :param ids: List of primary key ids
    """
    if GAE_PROJECT not in _ALLOWED_PROJECTS:
        return None

    if action not in ['insert', 'update', 'delete', 'upsert']:
        logging.error('Invalid database action value')
        return None

    if not ids or not isinstance(ids, (list, tuple)):
        logging.error('Ids argument is invalid or empty')
        return None

    # Both "int_ids" and "str_ids" must be list objects even if there are no values.
    int_ids = list()
    str_ids = list()
    if isinstance(ids[0], int):
        int_ids = ids
    else:
        str_ids = [str(i) for i in ids]

    # Publish PubSub event for new RDR to PDR pipeline
    topic = GCPGooglePubSubTopic(GAE_PROJECT, 'data-pipeline')
    # Payload data will be validated by the pub-sub topic schema.
    data = {
        "instance": _INSTANCE_MAPPING[GAE_PROJECT],
        "database": "rdr",
        "table": table,
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "pk_column": pk_column,
        "int_ids": int_ids,
        "str_ids": str_ids
    }
    try:
        resp = retry_func(topic.publish, backoff_amount=0.1, message=data)
        return resp
    except errors.HttpError as e:
        logging.error(e)

    return None