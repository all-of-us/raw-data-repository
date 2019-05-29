import collections
import datetime

import mock
from clock import FakeClock
from dao.ehr_dao import EhrReceiptDao
from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
import offline.update_ehr_status
from model.hpo import HPO
from model.organization import Organization
from participant_enums import EhrStatus
from test.unit_test.unit_test_util import FlaskTestBase, run_deferred_tasks


class UpdateEhrStatusTestCase(FlaskTestBase):
  def setUp(self, **kwargs):
    super(UpdateEhrStatusTestCase, self).setUp(use_mysql=True, **kwargs)
    FlaskTestBase.doSetUp(self)
    self.hpo_dao = HPODao()
    self.org_dao = OrganizationDao()
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()

    self.hpo_foo = self._make_hpo(int_id=10, string_id='hpo_foo')
    self.hpo_bar = self._make_hpo(int_id=11, string_id='hpo_bar')

    self.org_foo_a = self._make_org(hpo=self.hpo_foo, int_id=10, external_id='FOO_A')
    self.org_foo_b = self._make_org(hpo=self.hpo_foo, int_id=11, external_id='FOO_B')
    self.org_bar_a = self._make_org(hpo=self.hpo_bar, int_id=12, external_id='BAR_A')

    self.participants = [
      self._make_participant(hpo=self.hpo_foo, org=self.org_foo_a, int_id=11),
      self._make_participant(hpo=self.hpo_foo, org=self.org_foo_b, int_id=12),
      self._make_participant(hpo=self.hpo_bar, org=self.org_bar_a, int_id=13),
      self._make_participant(hpo=self.hpo_bar, org=self.org_bar_a, int_id=14),
    ]

  def tearDown(self):
    super(UpdateEhrStatusTestCase, self).tearDown()
    FlaskTestBase.doTearDown(self)

  def _make_hpo(self, int_id, string_id):
    hpo = HPO(hpoId=int_id, name=string_id)
    self.hpo_dao.insert(hpo)
    return hpo

  def _make_org(self, hpo, int_id, external_id):
    org = Organization(
      organizationId=int_id,
      externalId=external_id,
      displayName='SOME ORG',
      hpoId=hpo.hpoId
    )
    self.org_dao.insert(org)
    return org

  def _make_participant(self, hpo, org, int_id):
    participant = self._participant_with_defaults(participantId=int_id, biobankId=int_id)
    participant.hpoId = hpo.hpoId
    participant.organizationId = org.organizationId
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    self.summary_dao.insert(summary)
    return participant, summary

  EhrUpdatePidRow = collections.namedtuple('EhrUpdatePidRow', [
    'person_id',
    'report_run_time',
    'org_id',
    'hpo_id',
    'site_name',
  ])

  @mock.patch('offline.update_ehr_status.cloud_utils.bigquery.BigQueryJob')
  def test_updates_participant_summaries(self, mock_job):
    mock_job.return_value.__iter__.return_value = [
      [
        self.EhrUpdatePidRow(11, datetime.datetime(2019, 1, 1),
                             self.org_foo_a.organizationId,
                             self.hpo_foo.hpoId,
                             'foo_a_site_a'),
      ]
    ]

    with FakeClock(datetime.datetime(2019, 1, 1)):
      offline.update_ehr_status.update_ehr_status()

    mock_job.return_value.__iter__.return_value = [
      [
        self.EhrUpdatePidRow(11, datetime.datetime(2019, 1, 2),
                             self.org_foo_a.organizationId,
                             self.hpo_foo.hpoId,
                             'foo_a_site_a'),
        self.EhrUpdatePidRow(12, datetime.datetime(2019, 1, 2),
                             self.org_foo_b.organizationId,
                             self.hpo_foo.hpoId,
                             'foo_b_site_a'),
      ]
    ]

    with FakeClock(datetime.datetime(2019, 1, 2)):
      offline.update_ehr_status.update_ehr_status()

    summary = self.summary_dao.get(11)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,1))

    summary = self.summary_dao.get(12)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,2))
