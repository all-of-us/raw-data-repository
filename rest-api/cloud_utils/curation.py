from googleapiclient.discovery import build
from google.appengine.api import app_identity


SOCKET_TIMEOUT = 600000
BQ_DEFAULT_RETRY_COUNT = 10


def create_service():
  return build('bigquery', 'v2')


def query(
  q, app_id=None, dataset_id=None,
  use_legacy_sql=False,
  destination_dataset_id=None, destination_table_id=None,
  retry_count=BQ_DEFAULT_RETRY_COUNT,
  write_disposition='WRITE_EMPTY'
):
  """
  Execute a SQL query on BigQuery dataset

  NOTE: this was reworked from
        https://github.com/all-of-us/curation/blob/develop/data_steward/bq_utils.py

  :param q: SQL statement
  :param app_id: Default App for the query
  :param dataset_id: Default Dataset for the query
  :param use_legacy_sql: True if using legacy syntax, False by default
  :param destination_table_id: if set, output is saved in a table with the specified id
  :param retry_count: number of times to retry with randomized exponential backoff
  :param write_disposition: WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY (default)
  :param destination_dataset_id: dataset ID of destination table (EHR dataset by default)
  :return: if destination_table_id is supplied then job info, otherwise job query response
           (see https://goo.gl/AoGY6P and https://goo.gl/bQ7o2t)
  """
  bq_service = create_service()
  app_id = app_id or app_identity.get_application_id()

  if destination_table_id:
    raise NotImplemented("Writing to BigQuery has not been fleshed out yet.")
    job_body = {
      'configuration':
        {
          'query': {
            'query': q,
            'useLegacySql': use_legacy_sql,
            'defaultDataset': {
              'projectId': app_id,
              'datasetId': dataset_id
            },
            'destinationTable': {
              'projectId': app_id,
              'datasetId': destination_dataset_id,
              'tableId': destination_table_id
            },
            'writeDisposition': write_disposition
          }
        }
    }
    return (
      bq_service.jobs()
        .insert(projectId=app_id, body=job_body)
        .execute(num_retries=retry_count)
    )
  else:
    job_body = {
      'defaultDataset': {
        'projectId': app_id,
        'datasetId': dataset_id
      },
      'query': q,
      'timeoutMs': SOCKET_TIMEOUT,
      'useLegacySql': use_legacy_sql
    }
    return (
      bq_service.jobs()
        .query(projectId=app_id, body=job_body)
        .execute(num_retries=retry_count)
    )
