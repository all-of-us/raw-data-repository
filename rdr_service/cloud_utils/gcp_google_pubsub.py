#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Manage google PubSub services
# https://cloud.google.com/pubsub/docs/reference/rest
#
from datetime import datetime
from typing import List
import base64
import json
import logging

from apiclient import errors
from googleapiclient import discovery

from rdr_service.config import GAE_PROJECT
from rdr_service.services.system_utils import retry_func, list_chunks


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
                {'data': base64.b64encode(payload).decode('utf-8')}
            ]
        }

        req = self.service.projects().topics().publish(topic=self.topic_path, body=body)
        resp = req.execute()
        return resp

def _validate_pk_values(values_list: List, expected_len=1) -> List[List[str]] or None:
    """
    Helper function to verify/convert the pk_values into expected nested list of strings for the pubsub event
    :param values_list:  An object containing the primary key values
    :param expected_len: The number of RDR table columns that make up a valid primary key value.
    :return: The resulting nested list object, or None if data could not be validated and converted
    """
    if not len(values_list):
        return None

    # Examples of the pk_values (and pk_columns) content expected by the pubsub schema.  Uses all string values
    # 1. RDR participant table has a single column primary key, sample pubsub data specifying two records:
    #   pk_columns = ['participant_id']
    #   pk_values = [['012345678'], ['456789012']]
    # 2. RDR participant_history has a compound (2-column) primary key, sample pubsub data specifying three records:
    #   pk_columns = ['participant_id', 'version']
    #   pk_values = [['012345678', '1'], ['012345678', '2'] , ['456789012', '1']]

    converted_list = []
    # Convert a single-dimensional list of ints or strings to make it a nested list, per the required pubsub data schema
    if all(isinstance(v, (int, str)) for v in values_list):
        converted_list = [[str(val)] for val in values_list]
    # Received a list of lists/tuples, but confirm all elements of the sublists are ints or strings; convert to all str
    elif all(isinstance(v, (list, tuple)) for v in values_list):
        for sub_list in values_list:
            if all(isinstance(v, (int, str)) for v in sub_list):
                converted_list.append([str(el) for el in sub_list])
            else:
                return None
    else:
        # Couldn't apply conversions due to unexpected/mixed data types
        return None

    # Confirm each nested list element of primary key data has the expected number of items.  The expected_len should
    # be the number of table columns comprising the primary key (1 in most cases)
    for sub_list in converted_list:
        if len(sub_list) != expected_len:
            return None

    return converted_list


def publish_pdr_pubsub(table: str, action: str, pk_columns : List[str] = [],
                       pk_values: List = [], project=GAE_PROJECT):
    """
    Publish database table updates to the 'data-pipeline' pub-sub topic.
    :param table: Table name
    :param action: must be one of 'insert', 'update', 'delete' or 'upsert'.
    :param pk_columns: List of names of primary key columns in table
    :param pk_values: A list object containing the primary key values, which may represent compound primary keys.
                      If the caller did not provide the expected nested list, this method will attempt to convert
                      the pk_values data into its expected format for the pubsub data schema
    :param project:  The project name.  Default is the GAE_PROJECT from the local app config
    """
    last_response = None
    if project not in _ALLOWED_PROJECTS:
        return None

    if action not in ['insert', 'update', 'delete', 'upsert']:
        logging.error('Invalid database action value')
        return None

    if not len(pk_columns) or not all(isinstance(col, str) for col in pk_columns):
        logging.error(f'pk_columns list {pk_columns} is invalid or empty, only string values of column names expected')
        return None

    validated_pk_values = _validate_pk_values(pk_values, expected_len=len(pk_columns)) or []
    if not len(validated_pk_values):
        logging.error(f'pk_values argument {pk_values} contains invalid data types or is empty')
        return None

    # Publish PubSub event for new RDR to PDR pipeline. Payload data will be validated by the pub-sub topic schema.
    topic = GCPGooglePubSubTopic(project, 'data-pipeline')
    # Limit the number of pk_values passed in any pubsub event to 500 at a time
    for pk_values_batch in list_chunks(validated_pk_values, 500):
        data = {
            "instance": _INSTANCE_MAPPING[project],
            "database": "rdr",
            "table": table,
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "pk_columns": pk_columns,
            "pk_values": pk_values_batch
        }
        try:
            last_response = retry_func(topic.publish, backoff_amount=0.1, message=data)
            logging.info(f'Published pubsub event.  Response: {last_response}')
        except errors.HttpError as e:
            logging.error(e)

    return last_response
