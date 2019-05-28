import collections

from googleapiclient.discovery import build
from google.appengine.api import app_identity


class BigQueryJob(object):
  """
  Executes a BigQuery Job and handles iterating over result pages.

  DESIGNED FOR READ-ONLY USE AS OF 2019-05-28
  """

  def __init__(
    self,
    query,
    project_id=None,
    dataset_id=None,
    use_legacy_sql=False,
    retry_count=10,
    socket_timeout=10 * 60 * 1000,  # 10 minutes
    page_size=1000
  ):
    self.query = query
    self.project_id = project_id or app_identity.get_application_id()
    self.dataset_id = dataset_id
    self.use_legacy_sql = use_legacy_sql
    self.retry_count = retry_count
    self.socket_timeout = socket_timeout
    self.page_size = page_size
    self._service = build('bigquery', 'v2')

  def _make_job_body(self):
    return {
      'defaultDataset': {
        'projectId': self.project_id,
        'datasetId': self.dataset_id
      },
      'query': self.query,
      'timeoutMs': self.socket_timeout,
      'useLegacySql': self.use_legacy_sql,
      'maxResults': self.page_size,
    }

  def execute_and_iter_pages(self):
    service = build('bigquery', 'v2')
    job_body = self._make_job_body()
    result = (
      service.jobs()
        .query(projectId=self.project_id, body=job_body)
        .execute(num_retries=self.retry_count)
    )
    return self.iter_pages(service, result)

  def iter_pages(self, service, result):
    job_reference = result['jobReference']
    project_id = job_reference['projectId']
    job_id = job_reference['jobId']

    page = result
    while page:
      yield self._get_rows_from_response(page)
      page_token = page.get('pageToken')
      if page_token:
        page = (
          service.jobs()
            .getQueryResults(projectId=project_id, jobId=job_id,
                             pageToken=page_token, maxResults=self.page_size)
            .execute()
        )
      else:
        page = None

  @staticmethod
  def _get_rows_from_response(response):
    """
    Make a `list` of `Row` (namedtuple) objects representing the bigquery response dict structure.
    This list allows for cleaner usage of common result access patterns.

    :param response: Response `dict` structure from a call to `bigquery()`
    :rtype: list
    """
    field_names = [f['name'] for f in response['schema']['fields']]
    Row = collections.namedtuple('Row', field_names)
    return [
      Row(
        *[
          field['v']
          for field
          in row['f']
        ]
      )
      for row
      in response['rows']
    ]
