import os
import csv
import datetime
import mock

from rdr_service import config
from rdr_service.api_util import open_cloud_file, upload_from_string, list_blobs
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.model.participant import Participant
from rdr_service.model.hpo import HPO
from rdr_service.offline.export_va_workqueue import generate_workqueue_report, delete_old_reports

from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin


class ExportVaWorkQueueTest(BaseTestCase, PDRGeneratorTestMixin):

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
        self.subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)

    @mock.patch('rdr_service.offline.export_va_workqueue.clock.CLOCK')
    def test_export_va_workqueue(self, mock_clock):
        mock_clock.now.return_value = datetime.datetime(2022, 1, 13, 7, 4, 0)
        summary_dao = ParticipantSummaryDao()
        participant_dao = ParticipantDao()
        hpo_dao = HPODao()
        va_hpo = hpo_dao.insert(HPO(name="VA", hpoId=99))
        nids = 10
        for pid in range(nids):
            participant = participant_dao.insert(Participant())
            participant.hpoId = va_hpo.hpoId
            if pid == 2:
                participant.isGhostId = 0
            elif pid == 5:
                participant.isGhostId = 1
            elif pid == 7:
                participant.isTestParticipant = 1
            participant_dao.update(participant)
            summary_dao.insert(self.participant_summary(participant))
        generate_workqueue_report()
        with open_cloud_file(os.path.normpath(
            self.bucket + "/" + self.subfolder + "/va_daily_participant_wq_2022-01-13-07-04-00.csv")) as test_csv:
            reader = csv.DictReader(test_csv)
            row_count = sum(1 for _ in reader)
            self.assertEqual(row_count, nids - 2)

    @mock.patch('rdr_service.offline.export_va_workqueue.clock.CLOCK')
    def test_delete_old_reports(self, mock_clock):
        mock_clock.now.return_value = datetime.datetime(2022, 1, 13, 7, 4, 0)
        self.clear_default_storage()
        self.create_mock_buckets([self.bucket,
                                  self.bucket + "/" + self.subfolder])
        # Create files in bucket
        file_list = [
            "va_daily_participant_wq_2022-01-13-07-04-00.csv",
            "va_daily_participant_wq_2022-01-12-05-00-00.csv",
            "va_daily_participant_wq_2022-01-05-00-00-00.csv",
            "va_daily_participant_wq_2023-01-01-01-00-00.csv",
            "va_daily_participant_wq_2022-01-01-00-00-00.csv",
            "test.csv",
        ]
        for file in file_list:
            upload_from_string("test", self.bucket + "/" + self.subfolder + "/" + file)
        delete_old_reports()
        bucket_file_list = [file.name for file in list_blobs(self.bucket, self.subfolder)]
        self.assertIn(self.subfolder + "/va_daily_participant_wq_2022-01-12-05-00-00.csv", bucket_file_list)
        self.assertNotIn(self.subfolder + "/va_daily_participant_wq_2022-01-05-00-00-00.csv", bucket_file_list)
