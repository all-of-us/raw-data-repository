from datetime import datetime
from dateutil import parser
from flask import request

# from google.appengine.ext import deferred

from rdr_service.api_util import PTC_AND_HEALTHPRO
from rdr_service.api.base_api import BaseApi
from rdr_service.app_util import auth_required_task, auth_required
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_code_dao import deferrered_bq_codebook_update  # pylint: disable=unused-import
from rdr_service.dao.bq_hpo_dao import bq_hpo_update  # pylint: disable=unused-import
from rdr_service.dao.bq_organization_dao import bq_organization_update  # pylint: disable=unused-import
from rdr_service.dao.bq_site_dao import bq_site_update  # pylint: disable=unused-import
from rdr_service.offline.bigquery_sync import rebuild_bq_participant_task


class BQRebuildTaskApi(BaseApi):
    def __init__(self):
        super(BQRebuildTaskApi, self).__init__(BigQuerySyncDao())

    @auth_required_task
    def get(self):
        timestamp = parser.parse(
            request.args.get("timestamp", datetime.utcnow().isoformat())
        )
        limit = int(request.args.get("limit", 10000))
        rebuild_bq_participant_task(timestamp, limit=limit)
        return '{"success": "true"}'


@auth_required(PTC_AND_HEALTHPRO)
def rebuild_bigquery_core():
    """ On demand rebuild of Code, HPO, Organization and Sites tables for BigQuery """

    # Code Table
    # deferred.defer(deferrered_bq_codebook_update)
    # HPO Table
    # deferred.defer(bq_hpo_update)
    # Organization Table
    # deferred.defer(bq_organization_update)
    # Site Table
    # deferred.defer(bq_site_update)

    return '{"success": "true"}'
