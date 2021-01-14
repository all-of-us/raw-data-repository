import datetime
import pytz

from tests import test_data
from rdr_service.api_util import open_cloud_file
from rdr_service.offline import retention_eligible_import
from rdr_service import clock
from rdr_service.participant_enums import RetentionStatus, RetentionType
from tests.helpers.unittest_base import BaseTestCase

_FAKE_RETENTION_ELIGIBLE_BUCKET = "rdr_fake_retention_eligible_bucket"


class RetentionEligibleImportTest(BaseTestCase):
    def setUp(self):
        super(RetentionEligibleImportTest, self).setUp()

    def test_retention_eligible_import(self):
        ps1 = self.data_generator.create_database_participant_summary()
        ps2 = self.data_generator.create_database_participant_summary()
        ps3 = self.data_generator.create_database_participant_summary()
        ps4 = self.data_generator.create_database_participant_summary()
        ps5 = self.data_generator.create_database_participant_summary()

        participant_ids = [ps1.participantId, ps2.participantId, ps3.participantId, ps4.participantId]

        bucket_name = _FAKE_RETENTION_ELIGIBLE_BUCKET
        test_file = 'retention_test.csv'
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            self._create_ingestion_test_file(test_file, bucket_name, participant_ids)

        task_data = {
            "bucket": bucket_name,
            "upload_date": test_date.isoformat(),
            "file_path": bucket_name + '/' + test_file
        }

        retention_eligible_import.import_retention_eligible_metrics_file(task_data)

        psr1 = self.send_get(f'Participant/P{ps1.participantId}/Summary')
        self.assertEqual(psr1.get('retentionEligibleStatus'), str(RetentionStatus.ELIGIBLE))
        self.assertEqual(psr1.get('retentionEligibleTime'), '2020-02-20T13:45:00')
        self.assertEqual(psr1.get('retentionType'), str(RetentionType.ACTIVE))
        psr2 = self.send_get(f'Participant/P{ps2.participantId}/Summary')
        self.assertEqual(psr2.get('retentionEligibleStatus'), str(RetentionStatus.ELIGIBLE))
        self.assertEqual(psr2.get('retentionEligibleTime'), '2020-02-20T13:45:00')
        self.assertEqual(psr2.get('retentionType'), str(RetentionType.PASSIVE))
        psr3 = self.send_get(f'Participant/P{ps3.participantId}/Summary')
        self.assertEqual(psr3.get('retentionEligibleStatus'), str(RetentionStatus.ELIGIBLE))
        self.assertEqual(psr3.get('retentionEligibleTime'), '2020-02-20T13:45:00')
        self.assertEqual(psr3.get('retentionType'), str(RetentionType.ACTIVE_AND_PASSIVE))
        psr4 = self.send_get(f'Participant/P{ps4.participantId}/Summary')
        self.assertEqual(psr4.get('retentionEligibleStatus'), str(RetentionStatus.NOT_ELIGIBLE))
        self.assertEqual(psr4.get('retentionEligibleTime'), None)
        self.assertEqual(psr4.get('retentionType'), 'UNSET')
        psr5 = self.send_get(f'Participant/P{ps5.participantId}/Summary')
        self.assertEqual(psr5.get('retentionEligibleStatus'), 'UNSET')
        self.assertEqual(psr5.get('retentionEligibleTime'), None)
        self.assertEqual(psr5.get('retentionType'), 'UNSET')

        # test update with new file
        test_file = 'retention_test_2.csv'
        test_date = datetime.datetime(2020, 10, 14, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            self._create_ingestion_test_file(test_file, bucket_name, participant_ids)

        task_data = {
            "bucket": bucket_name,
            "upload_date": test_date.isoformat(),
            "file_path": bucket_name + '/' + test_file
        }

        retention_eligible_import.import_retention_eligible_metrics_file(task_data)

        psr1 = self.send_get(f'Participant/P{ps1.participantId}/Summary')
        self.assertEqual(psr1.get('retentionEligibleStatus'), str(RetentionStatus.ELIGIBLE))
        self.assertEqual(psr1.get('retentionEligibleTime'), '2020-03-20T13:45:00')
        self.assertEqual(psr1.get('retentionType'), str(RetentionType.PASSIVE))
        psr2 = self.send_get(f'Participant/P{ps2.participantId}/Summary')
        self.assertEqual(psr2.get('retentionEligibleStatus'), str(RetentionStatus.NOT_ELIGIBLE))
        self.assertEqual(psr2.get('retentionEligibleTime'), None)
        self.assertEqual(psr2.get('retentionType'), 'UNSET')
        psr3 = self.send_get(f'Participant/P{ps3.participantId}/Summary')
        self.assertEqual(psr3.get('retentionEligibleStatus'), str(RetentionStatus.ELIGIBLE))
        self.assertEqual(psr3.get('retentionEligibleTime'), '2020-02-20T13:45:00')
        self.assertEqual(psr3.get('retentionType'), str(RetentionType.ACTIVE_AND_PASSIVE))
        psr4 = self.send_get(f'Participant/P{ps4.participantId}/Summary')
        self.assertEqual(psr4.get('retentionEligibleStatus'), str(RetentionStatus.ELIGIBLE))
        self.assertEqual(psr4.get('retentionEligibleTime'), '2020-03-20T13:45:00')
        self.assertEqual(psr4.get('retentionType'), str(RetentionType.ACTIVE))
        psr5 = self.send_get(f'Participant/P{ps5.participantId}/Summary')
        self.assertEqual(psr5.get('retentionEligibleStatus'), 'UNSET')
        self.assertEqual(psr5.get('retentionEligibleTime'), None)
        self.assertEqual(psr5.get('retentionType'), 'UNSET')

        ps = self.send_get("ParticipantSummary?retentionEligibleStatus=NOT_ELIGIBLE&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionEligibleStatus=ELIGIBLE&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 3)
        ps = self.send_get("ParticipantSummary?retentionType=ACTIVE_AND_PASSIVE&retentionEligibleStatus=ELIGIBLE"
                           "&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionType=PASSIVE&retentionEligibleStatus=ELIGIBLE"
                           "&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionType=UNSET&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 2)
        ps = self.send_get("ParticipantSummary?retentionType=UNSET&retentionEligibleStatus=NOT_ELIGIBLE"
                           "&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)

    def _create_ingestion_test_file(self, test_data_filename, bucket_name, participant_ids, folder=None):
        test_data_file = self._open_test_file(test_data_filename, participant_ids)
        self._write_cloud_csv(test_data_filename, test_data_file, folder=folder, bucket=bucket_name)

    def _open_test_file(self, test_filename, participant_ids=None):
        with open(test_data.data_path(test_filename)) as f:
            lines = f.readlines()
            csv_str = ""
            for idx, line in enumerate(lines):
                if '{pid}' in line:
                    line = line.replace('{pid}', str(participant_ids[idx-1]))
                csv_str += line

            return csv_str

    def _write_cloud_csv(self, file_name, contents_str, bucket=None, folder=None):
        if folder is None:
            path = "/%s/%s" % (bucket, file_name)
        else:
            path = "/%s/%s/%s" % (bucket, folder, file_name)
        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))
