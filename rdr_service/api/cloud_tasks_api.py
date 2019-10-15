from datetime import datetime
from dateutil import parser
from flask import request

from rdr_service.api.base_api import BaseApi
from rdr_service.app_util import task_auth_required
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.offline.bigquery_sync import rebuild_bq_participant_task


class BQRebuildTaskApi(BaseApi):
    """
    Cloud Task endpoint: Rebuild all records for BigQuery.
    """
    def __init__(self):
        super(BQRebuildTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        timestamp = parser.parse(
            request.args.get("timestamp", datetime.utcnow().isoformat())
        )
        limit = int(request.args.get("limit", 300))
        rebuild_bq_participant_task(timestamp, limit=limit)
        return '{"success": "true"}'

