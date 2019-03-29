import datetime

from dao.calendar_dao import CalendarDao, INTERVAL_DAY, INTERVAL_WEEK, INTERVAL_MONTH, INTERVAL_QUARTER
from dao.ehr_dao import EhrReceiptDao
from dao.hpo_dao import HPODao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.organization_dao import OrganizationDao
from model.calendar import Calendar
from model.ehr import EhrReceipt
from model.hpo import HPO
from model.organization import Organization
from test.unit_test.unit_test_util import SqlTestBase


class EhrReceiptDaoTest(SqlTestBase):

  def setUp(self, with_data=True, use_mysql=True):
    super(EhrReceiptDaoTest, self).setUp(with_data=with_data, use_mysql=use_mysql)
    self.setup_fake()
    self.calendar_dao = CalendarDao()
    self.org_dao = OrganizationDao()
    self.hpo_dao = HPODao()
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()
    self._setup_initial_data()

  @staticmethod
  def _iter_dates_in_range(start, end):
    current = start
    while current <= end:
      yield current
      current += datetime.timedelta(days=1)

  def _fill_calendar_range(self, start, end):
    for date in self._iter_dates_in_range(start, end):
      self.calendar_dao.insert(Calendar(day=date))

  def _make_hpo(self, int_id, string_id):
    hpo = HPO(hpoId=int_id, name=string_id)
    self.hpo_dao.insert(hpo)
    return hpo

  def _make_org(self, **kwargs):
    org = Organization(**kwargs)
    self.org_dao.insert(org)
    return org

  def _make_participant(self, org, int_id):
    participant = self._participant_with_defaults(participantId=int_id, biobankId=int_id)
    participant.hpoId = org.hpoId
    participant.organizationId = org.organizationId
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    summary.hpoId = participant.hpoId
    summary.organizationId = participant.organizationId
    self.summary_dao.insert(summary)
    return participant, summary

  def _update_ehr(self, participant_summary, update_time):
    self.summary_dao.update_ehr_status(participant_summary, update_time)
    self.summary_dao.update(participant_summary)

  def _save_ehr_receipt(self, org, receipt_time):
    receipt = EhrReceipt(organizationId=org.organizationId, receiptTime=receipt_time)
    self.ehr_receipt_dao.insert(receipt)

  def _setup_initial_data(self):
    self.hpo_foo = self._make_hpo(int_id=10, string_id='hpo_foo')
    self.hpo_bar = self._make_hpo(int_id=11, string_id='hpo_bar')

    self.org_foo_a = self._make_org(
      organizationId=10,
      externalId='FOO_A',
      displayName='Foo A',
      hpoId=self.hpo_foo.hpoId
    )
    self.org_bar_a = self._make_org(
      organizationId=11,
      externalId='BAR_A',
      displayName='Bar A',
      hpoId=self.hpo_bar.hpoId
    )

    participant_and_summary_pairs = [
      self._make_participant(org=self.org_foo_a, int_id=11),
      self._make_participant(org=self.org_foo_a, int_id=12),
      self._make_participant(org=self.org_bar_a, int_id=13),
      self._make_participant(org=self.org_bar_a, int_id=14),
    ]
    self.participants = {
      participant.participantId: participant
      for participant, summary
      in participant_and_summary_pairs
    }
    self.summaries = {
      participant.participantId: summary
      for participant, summary
      in participant_and_summary_pairs
    }

  def test_get_active_organization_counts_in_interval_day(self):
    self._fill_calendar_range(datetime.date(2019, 1, 1), datetime.date(2019, 3, 1))

    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 2, 2))
    self._save_ehr_receipt(org=self.org_bar_a, receipt_time=datetime.datetime(2019, 2, 2))
    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 2, 4))

    results = self.ehr_receipt_dao.get_active_organization_counts_in_interval(
      start_date=datetime.datetime(2019, 2, 1),
      end_date=datetime.datetime(2019, 2, 7),
      interval=INTERVAL_DAY
    )

    self.assertEqual([(r['start_date'], r['active_organization_count']) for r in results], [
      (datetime.date(2019, 2, 1), 0L),
      (datetime.date(2019, 2, 2), 2L),
      (datetime.date(2019, 2, 3), 0L),
      (datetime.date(2019, 2, 4), 1L),
      (datetime.date(2019, 2, 5), 0L),
      (datetime.date(2019, 2, 6), 0L),
      (datetime.date(2019, 2, 7), 0L),
    ])

  def test_get_active_organization_counts_in_interval_week(self):
    self._fill_calendar_range(datetime.date(2019, 1, 1), datetime.date(2019, 3, 1))

    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 2, 4))
    self._save_ehr_receipt(org=self.org_bar_a, receipt_time=datetime.datetime(2019, 2, 4))
    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 2, 18))

    results = self.ehr_receipt_dao.get_active_organization_counts_in_interval(
      start_date=datetime.datetime(2019, 2, 1),
      end_date=datetime.datetime(2019, 3, 1),
      interval=INTERVAL_WEEK
    )

    self.assertEqual([(r['start_date'], r['active_organization_count']) for r in results], [
      (datetime.date(2019, 1, 27), 0L),
      (datetime.date(2019, 2, 3), 2L),
      (datetime.date(2019, 2, 10), 0L),
      (datetime.date(2019, 2, 17), 1L),
      (datetime.date(2019, 2, 24), 0L),
    ])

  def test_get_active_organization_counts_in_interval_month(self):
    self._fill_calendar_range(datetime.date(2018, 12, 1), datetime.date(2019, 7, 1))

    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 2, 1))
    self._save_ehr_receipt(org=self.org_bar_a, receipt_time=datetime.datetime(2019, 2, 1))
    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 4, 1))

    results = self.ehr_receipt_dao.get_active_organization_counts_in_interval(
      start_date=datetime.datetime(2019, 1, 1),
      end_date=datetime.datetime(2019, 5, 1),
      interval=INTERVAL_MONTH
    )

    self.assertEqual([(r['start_date'], r['active_organization_count']) for r in results], [
      (datetime.date(2019, 1, 1), 0L),
      (datetime.date(2019, 2, 1), 2L),
      (datetime.date(2019, 3, 1), 0L),
      (datetime.date(2019, 4, 1), 1L),
      (datetime.date(2019, 5, 1), 0L),
    ])

  def test_get_active_organization_counts_in_interval_quarter(self):
    self._fill_calendar_range(datetime.date(2018, 12, 1), datetime.date(2020, 1, 1))

    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 5, 1))
    self._save_ehr_receipt(org=self.org_bar_a, receipt_time=datetime.datetime(2019, 5, 1))
    self._save_ehr_receipt(org=self.org_foo_a, receipt_time=datetime.datetime(2019, 11, 1))

    results = self.ehr_receipt_dao.get_active_organization_counts_in_interval(
      start_date=datetime.datetime(2019, 1, 1),
      end_date=datetime.datetime(2020, 1, 1),
      interval=INTERVAL_QUARTER
    )

    self.assertEqual([(r['start_date'], r['active_organization_count']) for r in results], [
      (datetime.date(2019, 1, 1), 0L),
      (datetime.date(2019, 4, 1), 2L),
      (datetime.date(2019, 7, 1), 0L),
      (datetime.date(2019, 10, 1), 1L),
      (datetime.date(2020, 1, 1), 0L),
    ])

