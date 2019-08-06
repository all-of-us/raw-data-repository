from datetime import datetime

from dateutil import parser
from flask import request

from rdr_service.api.base_api import BaseApi
from rdr_service.app_util import auth_required_task
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.offline.bigquery_sync import rebuild_bigquery_data


class BQRebuildTaskApi(BaseApi):
    def __init__(self):
        super(BQRebuildTaskApi, self).__init__(BigQuerySyncDao())

    @auth_required_task
    def get(self):
        timestamp = parser.parse(request.args.get("timestamp", datetime.utcnow().isoformat()))
        limit = int(request.args.get("limit", 10000))
        rebuild_bigquery_data(timestamp, limit=limit)
        return '{"success": "true"}'
