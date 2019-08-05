import json

from google.appengine.api import app_identity
from werkzeug.exceptions import NotFound

from api.base_api import BaseApi
from rdr_service.api_util import PTC_AND_HEALTHPRO
from app_util import auth_required, nonprod
from cloud_utils.bigquery import BigQueryJob
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.model.bigquery_sync import BigQuerySync


class BQParticipantSummaryApi(BaseApi):
  def __init__(self):
    super(BQParticipantSummaryApi, self).__init__(BigQuerySyncDao())

  @nonprod
  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id):

    try:
      project_id = app_identity.get_application_id()
    except AttributeError:
      project_id = None

    if not project_id or project_id == 'None':
      # For local testing, return the json from mysql instead of making a bigquery call.
      with self.dao.session() as session:
        result = session.query(BigQuerySync.resource).filter(BigQuerySync.participantId == p_id).first()
        if result:
          return json.loads(result[0])

    else:
      # We must validate participant id carefully as we are building a query string, not using parameters.
      if str(p_id).isdigit() and len(str(p_id)) == 9:
        query = 'select * from rdr_ops_data_view.participant_summary where participant_id = {0}'.format(p_id)
        job = BigQueryJob(query, project_id=project_id, default_dataset_id='rdr_ops_data_view')

        response = job.start_job()
        if response and 'schema' in response and 'rows' in response and len(response['rows']) == 1:
          # logging.info(resource)
          records = BigQueryJob.get_rows(response)
          if len(records) == 1:
            return records[0]

    raise NotFound('participant not found')
