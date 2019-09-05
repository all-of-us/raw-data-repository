from datetime import datetime

from dateutil import parser
from flask import request

from api.base_api import BaseApi
from app_util import auth_required_task
from dao.bigquery_sync_dao import BigQuerySyncDao
from offline.bigquery_sync import rebuild_bq_participant_task


class BQRebuildTaskApi(BaseApi):
  def __init__(self):
    super(BQRebuildTaskApi, self).__init__(BigQuerySyncDao())

  @auth_required_task
  def get(self):
    timestamp = parser.parse(request.args.get('timestamp', datetime.utcnow().isoformat()))
    limit = int(request.args.get('limit', 10000))
    rebuild_bq_participant_task(timestamp, limit=limit)
    return '{"success": "true"}'
