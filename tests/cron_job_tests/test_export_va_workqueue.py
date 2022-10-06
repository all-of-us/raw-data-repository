import os
import json
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
from rdr_service.participant_enums import WithdrawalStatus

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
            if pid == 1:
                test_participant_id = str(participant.participantId)
            elif pid == 2:
                participant.isGhostId = 0
            elif pid == 5:
                participant.isGhostId = 1
            elif pid == 7:
                participant.isTestParticipant = 1
            participant_dao.update(participant)
            summary = summary_dao.insert(self.participant_summary(participant))
            if pid == 1:
                summary.dateOfBirth = datetime.date(1979, 3, 11)
                summary.questionnaireOnCopeDec = 1
                summary.questionnaireOnCopeDecAuthored = datetime.datetime(2022, 1, 3, 13, 23)
                summary.digitalHealthSharingStatus = {
                    'fitbit': {'status': 'YES', 'authoredTime': '2022-04-03T17:12:34Z'}
                }
                summary.patientStatus = [{'status': 'YES', 'organization': 'VA_BOSTON_VAMC'}]
                summary.consentCohort = 2
                summary.cohort2PilotFlag = 1
                summary.consentForElectronicHealthRecords = 1
                summary.ehrConsentExpireStatus = 2
                summary.primaryLanguage = 'en'
                summary.enrollmentStatus = 2

                summary_dao.update(summary)
            elif pid == 9:
                summary.withdrawalStatus = WithdrawalStatus.NO_USE
                summary.withdrawalTime = datetime.datetime(2019, 7, 11, 13, 2)
                summary_dao.update(summary)
        generate_workqueue_report()
        with open_cloud_file(
            os.path.normpath(self.bucket + "/" + self.subfolder + "/va_daily_participant_wq_2022-01-13-07-04-00.json")
        ) as test_file:
            report = json.load(test_file)

            self.assertEqual(len(report), nids - 2)
            for summary_json in report:
                if summary_json["participantId"] == "P" + test_participant_id:
                    self.assertEqual(summary_json["dateOfBirth"], "1979-03-11")
                    self.assertEqual(summary_json["questionnaireOnCopeDec"], "SUBMITTED")
                    self.assertEqual(summary_json["questionnaireOnCopeDecAuthored"], "2022-01-03T13:23:00")
                    self.assertEqual(summary_json["digitalHealthSharingStatus"]["fitbit"]["status"], "YES")
                    self.assertEqual(summary_json["digitalHealthSharingStatus"]["fitbit"]["authoredTime"],
                                     "2022-04-03T17:12:34Z")
                    self.assertEqual(summary_json["patientStatus"][0]["status"], "YES")
                    self.assertEqual(summary_json["patientStatus"][0]["organization"], "VA_BOSTON_VAMC")
                    self.assertEqual(summary_json["consentCohort"], "COHORT_2")
                    self.assertEqual(summary_json["cohort2PilotFlag"], "COHORT_2_PILOT")
                    self.assertEqual(summary_json["ehrConsentExpireStatus"], "EXPIRED")
                    self.assertEqual(summary_json["primaryLanguage"], "en")
                    self.assertEqual(summary_json["enrollmentStatus"], "MEMBER")

    @mock.patch('rdr_service.offline.export_va_workqueue.clock.CLOCK')
    def test_delete_old_reports(self, mock_clock):
        mock_clock.now.return_value = datetime.datetime(2022, 1, 13, 7, 4, 0)
        self.clear_default_storage()
        self.create_mock_buckets([self.bucket, self.bucket + "/" + self.subfolder])
        # Create files in bucket
        file_list = [
            "va_daily_participant_wq_2022-01-13-07-04-00.json",
            "va_daily_participant_wq_2022-01-12-05-00-00.json",
            "va_daily_participant_wq_2022-01-05-00-00-00.json",
            "va_daily_participant_wq_2023-01-01-01-00-00.json",
            "va_daily_participant_wq_2022-01-01-00-00-00.json",
            "test.json",
        ]
        for file in file_list:
            upload_from_string("test", self.bucket + "/" + self.subfolder + "/" + file)
        delete_old_reports()
        bucket_file_list = [file.name for file in list_blobs(self.bucket, self.subfolder)]
        self.assertIn(self.subfolder + "/va_daily_participant_wq_2022-01-12-05-00-00.json", bucket_file_list)
        self.assertNotIn(self.subfolder + "/va_daily_participant_wq_2022-01-05-00-00-00.json", bucket_file_list)
