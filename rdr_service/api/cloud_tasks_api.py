from datetime import datetime
from dateutil import parser
from flask import request

from werkzeug.exceptions import NotFound

from rdr_service.dao.bq_participant_summary_dao import bq_participant_summary_update_task
from rdr_service.api.data_gen_api import generate_samples_task
from rdr_service.dao.bq_questionnaire_dao import bq_questionnaire_update_task
from rdr_service.offline.sync_consent_files import cloudstorage_copy_objects_task
from rdr_service.api.base_api import BaseApi
from rdr_service.app_util import task_auth_required
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.offline.bigquery_sync import rebuild_bq_participant_task
from rdr_service.dao.bq_code_dao import rebuild_bq_codebook_task


class RebuildParticipantsBQTaskApi(BaseApi):
    """
    Cloud Task endpoint: Rebuild all participant records for BigQuery.
    """
    def __init__(self):
        super(RebuildParticipantsBQTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        timestamp = parser.parse(
            request.args.get("timestamp", datetime.utcnow().isoformat())
        )
        limit = int(request.args.get("limit", 300))
        rebuild_bq_participant_task(timestamp, limit=limit)
        return '{"success": "true"}'


class BQRebuildOneParticipantTaskApi(BaseApi):
    """
    Cloud Task endpoint: Rebuild one participant record for BigQuery.
    """
    def __init__(self):
        super(BQRebuildOneParticipantTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        p_id = int(request.args.get('p_id', '0'))
        if not p_id:
            raise NotFound('Invalid participant id')

        bq_participant_summary_update_task(p_id)
        return '{"success": "true"}'


class RebuildCodebookBQTaskApi(BaseApi):
    """
    Cloud Task endpoint: Rebuild Codebook records for BigQuery.
    """
    def __init__(self):
        super(RebuildCodebookBQTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        rebuild_bq_codebook_task()
        return '{"success": "true"}'


class CopyCloudStorageObjectTaskApi(BaseApi):
    """
    Cloud Task endpoint: Copy cloud storage object.
    """
    def __init__(self):
        super(CopyCloudStorageObjectTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        source = request.args.get('source', None)
        destination = request.args.get('destination', None)

        if not source or not destination:
            raise NotFound('Invalid cloud storage path: Copy {0} to {1}.'.format(source, destination))

        cloudstorage_copy_objects_task(source, destination)
        return '{"success": "true"}'


class BQRebuildQuestionnaireTaskApi(BaseApi):
    """
    Cloud Task endpoint: Rebuild questionnaire response for BigQuery.
    """
    def __init__(self):
        super(BQRebuildQuestionnaireTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        p_id = int(request.args.get('p_id', '0'))
        qr_id = int(request.args.get('qr_id', '0'))
        if not p_id:
            raise NotFound('Invalid participant id')
        if not qr_id:
            raise NotFound('Invalid questionnaire response id.')

        bq_questionnaire_update_task(p_id, qr_id)
        return '{"success": "true"}'


class GenerateBiobankSamplesTaskApi(BaseApi):
    """
    Cloud Task endpoint: Generate Biobank sample records.
    """
    def __init__(self):
        super(GenerateBiobankSamplesTaskApi, self).__init__(BigQuerySyncDao())

    @task_auth_required
    def get(self):
        fraction = float(request.args.get('fraction', '0.0'))
        generate_samples_task(fraction)
        return '{"success": "true"}'