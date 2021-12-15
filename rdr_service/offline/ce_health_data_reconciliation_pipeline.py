import datetime
import logging
import json

from rdr_service import clock
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.dao.database_utils import parse_datetime_from_iso_format
from rdr_service import config
from rdr_service.api_util import list_blobs, open_cloud_file, is_cloud_file_exits
from rdr_service.config import CE_HEALTH_DATA_BUCKET_NAME, RDR_SLACK_WEBHOOKS
from rdr_service.model.ce_health_reconciliation import CeHealthReconciliation
from rdr_service.dao.ce_health_reconciliation_dao import CeHealthReconciliationDao
from rdr_service.services.slack_utils import SlackMessageHandler

_RECONCILIATION_FILE_SUBDIR = 'reconciliation_reports'
_MISSING_REPORT_SUBDIR = 'missing_report'
_FILE_NAME_PREFIX = 'fitbit_reconciliation_report'
_MAX_INPUT_AGE = datetime.timedelta(hours=24)
_MAX_CHECK_WINDOW = datetime.timedelta(days=10)
_SLACK_WEBHOOK_NAME = 'rdr_ce_health_data_reconciliation_alerts'


class CeHealthDataReconciliationPipeline:
    def __init__(self):
        self.job_started_time = clock.CLOCK.now()
        self.bucket_name = config.getSetting(CE_HEALTH_DATA_BUCKET_NAME)
        self.slack_alert_helper = None

    def process_ce_manifest_files(self):
        logging.info('preparing slack alerts hook')
        slack_config = config.getSettingJson(RDR_SLACK_WEBHOOKS, {})
        webhook_url = slack_config.get(_SLACK_WEBHOOK_NAME)
        self.slack_alert_helper = SlackMessageHandler(webhook_url=webhook_url)
        logging.info('updating existing missing record for last 10 days')
        self._update_exist_missing_record_status()
        logging.info('processing the reconciliation files dropped in last 24 hours')

        prefix = _RECONCILIATION_FILE_SUBDIR + '/'
        reconciliation_files = self._find_latest_24_hours_reconciliation_files(prefix)
        for reconciliation_file in reconciliation_files:
            logging.info(f'parsing file: {self.bucket_name}/{reconciliation_file.name}')
            health_file_path_list, file_transferred_time = self._parse_reconciliation_file(reconciliation_file.name)
            missing_record_list = []
            for health_file_path in health_file_path_list:
                exist = is_cloud_file_exits(health_file_path)
                if not exist:
                    ce_health_reconciliation_record = CeHealthReconciliation(
                        missingFilePath=health_file_path,
                        status=False,
                        reportFilePath=self.bucket_name + '/' + reconciliation_file.name,
                        reportDate=reconciliation_file.updated,
                        fileTransferredTime=file_transferred_time
                    )
                    missing_record_list.append(ce_health_reconciliation_record)
            self._upsert_missing_file_records(missing_record_list)

    def generate_missing_report(self):
        exporter = SqlExporter(self.bucket_name)
        report_time_str = self.job_started_time.strftime('%Y_%m_%d_%H_%M_%S')
        report_path = f'{_MISSING_REPORT_SUBDIR}/missing_report_{report_time_str}.csv'
        logging.info(f"Writing {self.bucket_name}/{report_path} report.")

        ten_days_ago = self.job_started_time - _MAX_CHECK_WINDOW
        ce_health_reconciliation_dao = CeHealthReconciliationDao()
        with ce_health_reconciliation_dao.session() as session:
            records = ce_health_reconciliation_dao.get_missing_records_by_report_date(session, ten_days_ago)
            if records:
                missing_sql = """
                    SELECT missing_file_path, file_transferred_time, report_file_path, report_date
                    FROM ce_health_reconciliation WHERE report_date >= :ten_days_ago AND status IS FALSE
                """
                exporter.run_export(
                    report_path,
                    missing_sql,
                    {'ten_days_ago': ten_days_ago},
                    backup=False,
                    predicate=None
                )
                logging.info(f"uploaded missing report: {report_path}")
                self._send_missing_file_alert(report_path)

    def _send_missing_file_alert(self, missing_report_path):
        logging.info('sending missing report alert')
        message_data = {'text': f'CE health data missing files found, please check the report: {missing_report_path}'}
        self.slack_alert_helper.send_message_to_webhook(message_data=message_data)

    def _send_no_reconciliation_file_alert(self):
        logging.info('sending no reconciliation file alert')
        message_data = {'text': f'No CE health data reconciliation file found for last 24 hours, '
                                f'check time: {str(self.job_started_time)}'}
        self.slack_alert_helper.send_message_to_webhook(message_data=message_data)

    def _find_latest_24_hours_reconciliation_files(self, prefix):
        bucket_stat_list = list_blobs(self.bucket_name, prefix)
        if not bucket_stat_list:
            self._send_no_reconciliation_file_alert()
            raise RuntimeError("No files in last 24 hours in cloud bucket %r." % self.bucket_name)
        bucket_stat_list = [s for s in bucket_stat_list
                            if s.name.lower().endswith(".json") and _FILE_NAME_PREFIX in s.name]
        if not bucket_stat_list:
            self._send_no_reconciliation_file_alert()
            raise RuntimeError("No reconciliation files in cloud bucket %r (all files: %s)." % (self.bucket_name,
                                                                                                bucket_stat_list))
        last_24_hours_files = [blob for blob in bucket_stat_list
                               if blob.updated >= (self.job_started_time - _MAX_INPUT_AGE)]
        if not last_24_hours_files:
            self._send_no_reconciliation_file_alert()

        return last_24_hours_files

    def _parse_reconciliation_file(self, blob_name):
        file_path = self.bucket_name + '/' + blob_name
        health_file_path_list = []
        file_transferred_time = None
        with open_cloud_file(file_path) as reconciliation_file:
            content = json.load(reconciliation_file)
            if 'FileKeys' in content:
                for item in content['FileKeys']:
                    health_file_path_list.append(self.bucket_name + '/' + item)
            if 'TransferTime' in content:
                file_transferred_time = parse_datetime_from_iso_format(content['TransferTime'])

        return health_file_path_list, file_transferred_time

    def _upsert_missing_file_records(self, missing_record_list):
        ce_health_reconciliation_dao = CeHealthReconciliationDao()
        with ce_health_reconciliation_dao.session() as session:
            ce_health_reconciliation_dao.upsert_all_with_session(session, missing_record_list)

    def _update_exist_missing_record_status(self):
        ce_health_reconciliation_dao = CeHealthReconciliationDao()
        ten_days_ago = self.job_started_time - _MAX_CHECK_WINDOW
        with ce_health_reconciliation_dao.session() as session:
            records = ce_health_reconciliation_dao.get_missing_records_by_report_date(session, ten_days_ago)
            for record in records:
                exist = is_cloud_file_exits(record.missingFilePath)
                if exist:
                    record.status = True
