import mock
import json
import csv
from collections import namedtuple
from datetime import datetime, timedelta

from rdr_service.clock import FakeClock
from rdr_service import config
from rdr_service.api_util import open_cloud_file
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin
from rdr_service.offline.ce_health_data_reconciliation_pipeline import CeHealthDataReconciliationPipeline, \
    _RECONCILIATION_FILE_SUBDIR, _MISSING_REPORT_SUBDIR, _SLACK_WEBHOOK_NAME
from rdr_service.model.ce_health_reconciliation import CeHealthReconciliation
from rdr_service.dao.ce_health_reconciliation_dao import CeHealthReconciliationDao
from tests import test_data

_FAKE_BUCKET = "rdr_fake_bucket"
FakeBlob = namedtuple('FakeBlob', ['name', 'updated'])

TIME_1 = datetime(2021, 12, 10, 1, 30, 30)
TIME_2 = datetime(2021, 11, 10, 2, 20, 20)


class CeHealthDataReconciliationPipelineTest(BaseTestCase, PDRGeneratorTestMixin):
    def setUp(self):
        super(CeHealthDataReconciliationPipelineTest, self).setUp()
        config.override_setting(config.CE_HEALTH_DATA_BUCKET_NAME, [_FAKE_BUCKET])
        config.override_setting(config.RDR_SLACK_WEBHOOKS, {_SLACK_WEBHOOK_NAME: 'fake_slack_webhook_url'})

    def tearDown(self):
        # reload config to remove the override impact
        self.setup_config()

    def _write_cloud_csv(self, file_name, contents_str):
        with open_cloud_file("/%s/%s" % (_FAKE_BUCKET, file_name), mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))

    @mock.patch('rdr_service.offline.ce_health_data_reconciliation_pipeline.list_blobs')
    def test_reconciliation_pipeline(self, mock_list_blobs):
        pipeline = CeHealthDataReconciliationPipeline()

        # add two missing records into the tables,
        # make sure the last 10 days missing records will be recheck during running the pipeline
        missing_record_list = []
        ce_health_reconciliation_record_1 = CeHealthReconciliation(
            missingFilePath=_FAKE_BUCKET + '/' + 'raw/missing/1.json',
            status=False,
            reportFilePath=_FAKE_BUCKET + '/' + 'reconciliation_1.json',
            reportDate=TIME_1,
            fileTransferredTime=TIME_2
        )
        ce_health_reconciliation_record_2 = CeHealthReconciliation(
            missingFilePath=_FAKE_BUCKET + '/' + 'raw/missing/2.json',
            status=False,
            reportFilePath=_FAKE_BUCKET + '/' + 'reconciliation_2.json',
            reportDate=TIME_1,
            fileTransferredTime=TIME_2
        )
        missing_record_list.append(ce_health_reconciliation_record_1)
        missing_record_list.append(ce_health_reconciliation_record_2)
        ce_health_reconciliation_dao = CeHealthReconciliationDao()
        with ce_health_reconciliation_dao.session() as session:
            ce_health_reconciliation_dao.upsert_all_with_session(session, missing_record_list)

        mock_list_blobs.return_value = [
            FakeBlob(name=_RECONCILIATION_FILE_SUBDIR + '/fitbit_reconciliation_report_2021-12-09T10:10:10Z.json',
                     updated=TIME_1),
            FakeBlob(name=_RECONCILIATION_FILE_SUBDIR + '/fitbit_reconciliation_report_2021-11-09T10:10:10Z.json',
                     updated=TIME_1),
            # a reconciliation file which dropped 24 hours ago, will be ignored
            FakeBlob(name=_RECONCILIATION_FILE_SUBDIR + '/fitbit_reconciliation_report_2021-10-09T10:10:10Z.json',
                     updated=TIME_2),
            FakeBlob(name='raw/health/2021/11/18/FITBIT/activities-heart-intraday/P1234567890/'
                          'activities_heart_date_2021-11-07_1d_1sec.json',
                     updated=TIME_2),
            FakeBlob(name='raw/health/2021/11/18/FITBIT/activities-heart-intraday/P1234567890/'
                          'activities_heart_date_2021-11-05_1d_1sec.json',
                     updated=TIME_2),
            # add these two files, so they will not be in the missing report again
            FakeBlob(name='raw/missing/1.json',
                     updated=TIME_2),
            FakeBlob(name='raw/missing/2.json',
                     updated=TIME_2)

            # this file is not added: raw/health/2021/11/18/FITBIT/activities-heart-intraday/P1234567890/
            # activities_heart_date_2021-11-10_1d_1sec.json, this one will be in the missing report

            # this file is not added: raw/health/2021/11/18/FITBIT/activities-heart-intraday/P1234567890/
            # activities_heart_date_2021-11-20_1d_1sec.json, but this one is in a reconciliation file which dropped
            # 24 hours ago,this will not be in the missing report
        ]

        # multiple reconciliation files dropped in last 24 hours
        reconciliation_file_content_1 = test_data.load_test_data_json("fitbit_reconciliation_report_1.json")
        json_str_1 = json.dumps(reconciliation_file_content_1, indent=4)
        reconciliation_file_content_2 = test_data.load_test_data_json("fitbit_reconciliation_report_2.json")
        json_str_2 = json.dumps(reconciliation_file_content_2, indent=4)
        reconciliation_file_content_3 = test_data.load_test_data_json("fitbit_reconciliation_report_3.json")
        json_str_3 = json.dumps(reconciliation_file_content_3, indent=4)
        # write test data to the cloud files
        self._write_cloud_csv('reconciliation/fitbit_reconciliation_report_2021-12-09T10:10:10Z.json', json_str_1)
        self._write_cloud_csv('reconciliation/fitbit_reconciliation_report_2021-11-09T10:10:10Z.json', json_str_2)
        self._write_cloud_csv('reconciliation/fitbit_reconciliation_report_2021-10-09T10:10:10Z.json', json_str_3)
        self._write_cloud_csv('raw/health/2021/11/18/FITBIT/activities-heart-intraday/P1234567890/'
                              'activities_heart_date_2021-11-07_1d_1sec.json',
                              'test_content')
        self._write_cloud_csv('raw/health/2021/11/18/FITBIT/activities-heart-intraday/P1234567890/'
                              'activities_heart_date_2021-11-05_1d_1sec.json',
                              'test_content')
        self._write_cloud_csv('raw/missing/1.json',
                              'test_content')
        self._write_cloud_csv('raw/missing/2.json',
                              'test_content')

        later_in_24_hours = TIME_1 + timedelta(hours=23)
        with FakeClock(later_in_24_hours):
            # run the pipeline
            pipeline.process_ce_manifest_files()
            pipeline.generate_missing_report()

            missing_report_time_str = pipeline.job_started_time.strftime('%Y_%m_%d_%H_%M_%S')
            missing_report_path = _FAKE_BUCKET + '/' + _MISSING_REPORT_SUBDIR + '/' + 'missing_report_' \
                                  + missing_report_time_str + '.csv'
            with open_cloud_file(missing_report_path) as missing_report_file:
                csv_reader = csv.DictReader(missing_report_file, delimiter=",")
                rows = []
                for row in csv_reader:
                    rows.append(row)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['missing_file_path'], 'rdr_fake_bucket/raw/health/2021/11/18/FITBIT/'
                                                               'activities-heart-intraday/P1234567890/'
                                                               'activities_heart_date_2021-11-10_1d_1sec.json')

                self.assertEqual(rows[0]['file_transferred_time'], '2021-11-18 17:24:27.325256')
                self.assertEqual(rows[0]['report_file_path'], 'rdr_fake_bucket/reconciliation/'
                                                              'fitbit_reconciliation_report_2021-12-09T10:10:10Z.json')
                self.assertEqual(rows[0]['report_date'], '2021-12-10 01:30:30')
