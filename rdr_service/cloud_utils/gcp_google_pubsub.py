#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Manage google PubSub services
# https://cloud.google.com/pubsub/docs/reference/rest
#
from datetime import datetime
from dictalchemy import DictableModel
from typing import List
import base64
import json
import logging

from apiclient import errors
from googleapiclient import discovery
from sqlalchemy import inspect

from rdr_service import config
from rdr_service.config import GAE_PROJECT
from rdr_service.services.system_utils import retry_func, list_chunks


_INSTANCE_MAPPING = {
    'all-of-us-rdr-prod': 'all-of-us-rdr-prod:us-central1:rdrbackupdb-b',
    'all-of-us-rdr-stable': 'all-of-us-rdr-stable:us-central1:rdrbackupdb',
    'pmi-drc-api-test': 'pmi-drc-api-test:us-central1:rdrmaindb',
    'all-of-us-rdr-sandbox': 'all-of-us-rdr-sandbox:us-central1:rdrmaindb'
}

# TODO: Add production project id after testing in Stable.
# TODO: Add Stable after more testing has been completed in Test.

# _PIPELINE_CONFIG = config.getSettingJson('pdr_pipeline')
# _ALLOWED_PROJECTS = _PIPELINE_CONFIG['allowed_projects']
# PUBSUB_EXCLUDED_TABLE_LIST = _PIPELINE_CONFIG['excluded_table_list']

def publish_pubsub_message(project_id: str, topic: str, message: dict):
    """
    Publish a pub-sub topic message. Should only be called by submit_pipeline_pubsub_msg().
    https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
    :param message: Data to publish to pub-sub topic.
    :return: operation result
    """
    service = discovery.build('pubsub', 'v1', cache_discovery=True)
    topic_path = f'projects/{project_id}/topics/{topic}'
    payload = json.dumps(message).encode('utf-8')
    body = {
        "messages": [
            {'data': base64.b64encode(payload).decode('utf-8')}
        ]
    }
    req = service.projects().topics().publish(topic=topic_path, body=body)
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
    # Convert single-dimensional list of ints or strings to make it a nested list, per the required pubsub data schema
    if all(isinstance(v, (int, str)) for v in values_list):
        converted_list = [[str(val)] for val in values_list]
    # Received a list of lists/tuples, but confirm all elements of the sublists are ints or strings; convert all to str
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


def submit_pipeline_pubsub_msg(database: str = 'rdr', table: str = None, action: str = 'None',
                               pk_columns : List[str] = None, pk_values: List = None, project=GAE_PROJECT):
    """
    Publish database table updates to the 'data-pipeline' pub-sub topic by submitting a pub/sub message.
    # Note: We want this function to succeed/fail without raising any exceptions.
    :param database: Database name, default = 'rdr'.
    :param table: Table name
    :param action: must be one of 'insert', 'update', 'delete' or 'upsert'.
    :param pk_columns: List of names of primary key columns in table
    :param pk_values: A list object containing the primary key values, which may represent compound primary keys.
                      If the caller did not provide the expected nested list, this method will attempt to convert
                      the pk_values data into its expected format for the pubsub data schema
    :param project:  The project name.  Default is the GAE_PROJECT from the local app config
    """
    def log_error(msg: str, response_only=False):
        """ Log message and setup return dict """
        if response_only is False:
            logging.error(msg)
        resp = {'error': msg}
        return resp

    pdr_config = config.getSettingJson('pdr_pipeline')
    allowed_projects = pdr_config['allowed_projects']

    # If project is not allowed or is localhost, return error message.
    if project not in allowed_projects:
        return log_error(f'pipeline: project {project} not allowed.', True)

    if not database:
        database = 'rdr'
    if not table:
        return log_error('pipeline: invalid database table name')
    if action not in ['insert', 'update', 'delete', 'upsert']:
        return log_error('pipeline: invalid database action value')
    if not pk_columns or not all(isinstance(col, str) for col in pk_columns):
        return log_error(f'pipeline: primary key column list {pk_columns} is invalid or empty')
    if not pk_values or not isinstance(pk_values, (list, tuple)):
        return log_error(f'pipeline: primary key values argument is invalid or empty')

    # validate and prep the PK values list.
    validated_pk_values = _validate_pk_values(pk_values, expected_len=len(pk_columns)) or []
    if not validated_pk_values or len(validated_pk_values) == 0:
        log_error(f'pipeline: after validation {pk_values} is empty or contains invalid data')
    # Make sure that the length of the first value list is the same length as the PK columns list.
    if len(pk_columns) != len(validated_pk_values[0]):
        return log_error(f'pipeline: primary key columns and values are mismatched.')

    # Limit the number of pk_values passed in any pubsub event to 500 at a time
    count = 0
    last_response = {'error': 'pipeline: should not ever see this message'}
    for pk_values_batch in list_chunks(validated_pk_values, 500):
        # Warning: Do not change this structure without changing the defined schema in the pub/sub topic in GCP.
        data = {
            "instance": _INSTANCE_MAPPING[project],
            "database": database,
            "table": table,
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "pk_columns": pk_columns,
            "pk_values": pk_values_batch
        }
        try:
            # sample response: {'messageIds': ['6516999682321403']}
            last_response = retry_func(publish_pubsub_message, backoff_amount=0.1, project_id=project,
                                       topic='data-pipeline', message=data)
            count += 1
        except errors.HttpError as e:
            return log_error(str(e))

    logging.info(f'pipeline: submitted {count} pubsub messages.')
    return last_response


def submit_pipeline_pubsub_msg_from_model(models: [DictableModel, List[DictableModel]], database:str,
                                          parents: List[str] = None):
    """
    Take a SQLAlchemy model object with data and submit Pub/Sub messages for it and any
    child model objects. Recursive function.
    :param models: A DictableModel object or List of DictableModel objects with data
    :param database: Database name used by this model. See base_dao.py:get_connection_database_name()
    :param parents: A list of models we have already processed. This is used to make sure we don't enter a
                    circular recursion loop.
    """
    pdr_config = config.getSettingJson('pdr_pipeline')
    allowed_projects = pdr_config['allowed_projects']
    excluded_table_list = pdr_config['excluded_table_list']

    # Allow code to run in _ALLOWED_PROJECTS or if running locally for all unittests, otherwise just return.
    if GAE_PROJECT != 'localhost' and GAE_PROJECT not in allowed_projects:
        return None
    if not models:
        return parents
    if not parents:
        parents = list()

    def iter_children(m, r, p):
        # Recursively handle any child tables if present.  This is for API calls like BioBank orders
        # where the order sample records are saved to the database at the same time as the order record.
        if not r:
            return
        for relation in r:
            try:
                chld = getattr(m, relation.key, None)
            except Exception:  # pylint: disable=broad-except
                continue
            # 'chld' can be a list of model objects or a single model object.
            if chld:
                # Skip excluded tables.
                if isinstance(chld, list):
                    if chld[0].__tablename__ in excluded_table_list or chld[0].__tablename__ in p:
                        continue
                else:
                    if chld.__tablename__ in excluded_table_list or chld.__tablename__ in p:
                        continue

                submit_pipeline_pubsub_msg_from_model(chld, database=database, parents=p)

    if isinstance(models, DictableModel):
        models = [models]
    if not isinstance(models, List):
        return parents

    tablename = None
    pk_columns = None
    pk_values = list()

    # The models in the list will all be of the same model class. Pub/Sub messages for children
    # will be sent before the parent.
    for model in models:
        cls_mapper = inspect(model.__class__)
        tablename = model.__tablename__
        # Setup PK column names and PK values for this record.
        if not pk_columns:
            pk_columns = [c.name for c in cls_mapper.primary_key]
        # Get the primary key column values. Sometimes it's easy, sometimes we need to look at the object properties.
        ident = inspect(model).identity
        if not ident:
            # Try extracting the PK values by 'getting' the model object properties.
            ident = list()
            for pkc in pk_columns:
                try:
                    pc = next((k for k in cls_mapper.c.keys() if cls_mapper.c[k].name == pkc), None)
                except KeyError:
                    continue
                if pc:
                    v_ = getattr(model, pc)
                    if v_ is None:
                        # If one of the primary key values is null, then we don't want to add it to the pub/sub message.
                        break
                    ident.append(v_)
        if len(ident) != len(pk_columns):
            # If we reach this point, it's most likely that is an update to the parent record only and not the
            # child records. During testing, this is reached only during PUT API calls.
            continue
        pk_values.append(list(ident))
        # # It's Ok we are overwriting the PK values here, we want the child PK values to be correct.
        # parents[model.__tablename__] = dict(zip(pk_columns, pk_values))
        if model.__tablename__ not in parents:
            parents.append(model.__tablename__)
        # See if we have any child model records [ONE-TO-ONE or ONE-MANY] and submit pub/sub messages if we do.
        iter_children(model, cls_mapper.relationships, parents)

    # Submit a pipeline Pub/Sub event for this model record.
    submit_pipeline_pubsub_msg(database=database, table=tablename, action='upsert',
                               pk_columns=pk_columns,
                               pk_values=pk_values)

    return parents