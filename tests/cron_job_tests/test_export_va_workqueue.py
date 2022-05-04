import os
import csv
import mock
import datetime

from rdr_service import config
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.model.participant import Participant
from rdr_service.model.hpo import HPO
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin

from rdr_service.offline.export_va_workqueue import generate_workqueue_report


class VaWqExporterTest(BaseTestCase, PDRGeneratorTestMixin):

    def setUp(self):
        super().setUp()

    @mock.patch('rdr_service.offline.export_va_workqueue.clock.CLOCK')
    def testExportVaWorkQueue(self, mock_clock):
        mock_clock.now.return_value = datetime.datetime(2022, 1, 13, 7, 4, 0)
        summary_dao = ParticipantSummaryDao()
        participant_dao = ParticipantDao()
        hpo_dao = HPODao()
        va = hpo_dao.insert(HPO(name="VA", hpoId=99))
        nids = 10
        for x in range(nids):
            participant = participant_dao.insert(Participant())
            participant.hpoId = va.hpoId
            if x == 2:
                participant.isGhostId = 0
            elif x == 5:
                participant.isGhostId = 1
            elif x == 7:
                participant.isTestParticipant = 1
            participant_dao.update(participant)
            summary_dao.insert(self.participant_summary(participant))
        generate_workqueue_report()
        with open_cloud_file(os.path.normpath(config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)+"/Participants_2022-01-13-07-04-00.csv")) as f:
            reader = csv.DictReader(f)
            row_count = 0
            for _ in reader:
                row_count += 1
            self.assertEqual(row_count, nids - 2)
