import datetime

import mock
from clock import FakeClock
from dao.ehr_dao import EhrReceiptDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.ehr import EhrReceipt
import offline.update_ehr_status
from participant_enums import EhrStatus
from test.unit_test.unit_test_util import FlaskTestBase, run_deferred_tasks, CloudStorageSqlTestBase, TestBase


class UpdateEhrStatusTest(CloudStorageSqlTestBase, FlaskTestBase):
  """Tests behavior of sync_consent_files
  """
  def setUp(self, **kwargs):
    super(UpdateEhrStatusTest, self).setUp(use_mysql=True, **kwargs)
    FlaskTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    self.taskqueue.FlushQueue('default')
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()

  def _create_participant(self, id_):
    participant = self._participant_with_defaults(participantId=id_, biobankId=id_)
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    self.summary_dao.insert(summary)
    return participant

  @mock.patch('cloud_utils.curation.query')
  def test_create_ehr_receipts(self, mock_query):
    participant = self._participant_with_defaults(
      participantId=10,
      biobankId=10,
      siteId=1
    )
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    self.summary_dao.insert(summary)
    self.assertEqual(summary.ehrStatus, EhrStatus.NOT_PRESENT)

    time_1 = datetime.datetime(2019, 2, 20, 10, 15)
    mock_query.return_value = iter([
      {
        "person_id": "10",
        "report_run_time": time_1.isoformat(),
        "org_id": "AZ_TUCSON_BANNER_HEALTH",
        "hpo_id": "UAMC_BANNER",
        "site_name": "Banner Health"
      }
    ])

    with FakeClock(time_1):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    time_2 = datetime.datetime(2019, 2, 25, 10, 15)
    mock_query.return_value = iter([
      {
        "person_id": "10",
        "report_run_time": time_2.isoformat(),
        "org_id": "AZ_TUCSON_BANNER_HEALTH",
        "hpo_id": "UAMC_BANNER",
        "site_name": "Banner Health"
      }
    ])

    with FakeClock(time_2):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    result_summary = self.summary_dao.get(participant.participantId)
    self.assertEqual(result_summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(result_summary.ehrReceiptTime, time_1)
    self.assertEqual(result_summary.ehrUpdateTime, time_2)
    with self.ehr_receipt_dao.session() as session:
      receipts = session.query(EhrReceipt).all()
    self.assertEqual(len(receipts), 2)
    self.assertEqual(receipts[0].participantId, participant.participantId)
    self.assertEqual(receipts[0].recordedTime, time_1)  # NOTE: address this when fixed upstream
    self.assertEqual(receipts[0].receivedTime, time_1)
    self.assertEqual(receipts[1].participantId, participant.participantId)
    self.assertEqual(receipts[1].recordedTime, time_2)  # NOTE: address this when fixed upstream
    self.assertEqual(receipts[1].receivedTime, time_2)


#TODO: test querying BigQuery, this might belong in a different test file
