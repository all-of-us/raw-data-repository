import datetime

from dao.calendar_dao import CalendarDao, INTERVAL_DAY, INTERVAL_WEEK, INTERVAL_MONTH, INTERVAL_QUARTER
from dao.ehr_dao import EhrReceiptDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.site_dao import SiteDao
from model.calendar import Calendar
from model.participant import Participant
from test.unit_test.unit_test_util import SqlTestBase


class EhrReceiptDaoTest(SqlTestBase):

  def setUp(self, with_data=True, use_mysql=True):
    super(EhrReceiptDaoTest, self).setUp(with_data=with_data, use_mysql=use_mysql)
    self.setup_fake()
    self.calendar_dao = CalendarDao()
    self.site_dao = SiteDao()
    self.participant_dao = ParticipantDao()
    self.participant_summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()

  def _iter_dates_in_range(self, start, end):
    current = start
    while current <= end:
      yield current
      current += datetime.timedelta(days=1)

  def _fill_calendar_range(self, start, end):
    for date in self._iter_dates_in_range(start, end):
      self.calendar_dao.insert(Calendar(day=date))

  def test_get_active_site_counts_in_interval_day(self):
    #
    # create minimum data needed for test
    #
    self._fill_calendar_range(datetime.date(2019, 1, 1), datetime.date(2019, 3, 1))

    site_1 = self.site_dao.get(1)
    site_2 = self.site_dao.get(2)

    participant_1 = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant_1)
    summary_1 = self.participant_summary(participant_1)
    summary_1.siteId = site_1.siteId
    self.participant_summary_dao.insert(summary_1)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 2),
      received_time=datetime.datetime(2019, 2, 3)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 4),
      received_time=datetime.datetime(2019, 2, 5)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 4),
      received_time=datetime.datetime(2019, 2, 5)
    ))
    self.participant_summary_dao.update(summary_1)

    participant_2 = Participant(participantId=2, biobankId=3)
    self.participant_dao.insert(participant_2)
    summary_2 = self.participant_summary(participant_2)
    summary_2.siteId = site_2.siteId
    self.participant_summary_dao.insert(summary_2)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_2,
      recorded_time=datetime.datetime(2019, 2, 2),
      received_time=datetime.datetime(2019, 2, 3)
    ))
    self.participant_summary_dao.update(summary_2)

    #
    # begin test
    #
    results = self.ehr_receipt_dao.get_active_site_counts_in_interval(
      start_date=datetime.datetime(2019, 2, 1),
      end_date=datetime.datetime(2019, 2, 7),
      interval=INTERVAL_DAY
    )
    #import pprint
    #pprint.pprint(results)

    self.assertEqual([(r['start_date'], r['active_site_count']) for r in results], [
      (datetime.date(2019, 2, 1), 0L),
      (datetime.date(2019, 2, 2), 2L),
      (datetime.date(2019, 2, 3), 0L),
      (datetime.date(2019, 2, 4), 1L),
      (datetime.date(2019, 2, 5), 0L),
      (datetime.date(2019, 2, 6), 0L),
      (datetime.date(2019, 2, 7), 0L),
    ])

  def test_get_active_site_counts_in_interval_week(self):
    #
    # create minimum data needed for test
    #
    self._fill_calendar_range(datetime.date(2019, 1, 1), datetime.date(2019, 3, 1))

    site_1 = self.site_dao.get(1)
    site_2 = self.site_dao.get(2)

    participant_1 = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant_1)
    summary_1 = self.participant_summary(participant_1)
    summary_1.siteId = site_1.siteId
    self.participant_summary_dao.insert(summary_1)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 4),
      received_time=datetime.datetime(2019, 2, 5)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 18),
      received_time=datetime.datetime(2019, 2, 19)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 18),
      received_time=datetime.datetime(2019, 2, 19)
    ))
    self.participant_summary_dao.update(summary_1)

    participant_2 = Participant(participantId=2, biobankId=3)
    self.participant_dao.insert(participant_2)
    summary_2 = self.participant_summary(participant_2)
    summary_2.siteId = site_2.siteId
    self.participant_summary_dao.insert(summary_2)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_2,
      recorded_time=datetime.datetime(2019, 2, 4),
      received_time=datetime.datetime(2019, 2, 5)
    ))
    self.participant_summary_dao.update(summary_2)

    #
    # begin test
    #
    results = self.ehr_receipt_dao.get_active_site_counts_in_interval(
      start_date=datetime.datetime(2019, 2, 1),
      end_date=datetime.datetime(2019, 3, 1),
      interval=INTERVAL_WEEK
    )
    #import pprint
    #pprint.pprint(results)

    self.assertEqual([(r['start_date'], r['active_site_count']) for r in results], [
      (datetime.date(2019, 1, 27), 0L),
      (datetime.date(2019, 2, 3), 2L),
      (datetime.date(2019, 2, 10), 0L),
      (datetime.date(2019, 2, 17), 1L),
      (datetime.date(2019, 2, 24), 0L),
    ])

  def test_get_active_site_counts_in_interval_month(self):
    #
    # create minimum data needed for test
    #
    self._fill_calendar_range(datetime.date(2018, 12, 1), datetime.date(2019, 7, 1))

    site_1 = self.site_dao.get(1)
    site_2 = self.site_dao.get(2)

    participant_1 = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant_1)
    summary_1 = self.participant_summary(participant_1)
    summary_1.siteId = site_1.siteId
    self.participant_summary_dao.insert(summary_1)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 2, 1),
      received_time=datetime.datetime(2019, 2, 2)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 4, 1),
      received_time=datetime.datetime(2019, 4, 2)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 4, 1),
      received_time=datetime.datetime(2019, 4, 2)
    ))
    self.participant_summary_dao.update(summary_1)

    participant_2 = Participant(participantId=2, biobankId=3)
    self.participant_dao.insert(participant_2)
    summary_2 = self.participant_summary(participant_2)
    summary_2.siteId = site_2.siteId
    self.participant_summary_dao.insert(summary_2)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_2,
      recorded_time=datetime.datetime(2019, 2, 1),
      received_time=datetime.datetime(2019, 2, 2)
    ))
    self.participant_summary_dao.update(summary_2)

    #
    # begin test
    #
    results = self.ehr_receipt_dao.get_active_site_counts_in_interval(
      start_date=datetime.datetime(2019, 1, 1),
      end_date=datetime.datetime(2019, 5, 1),
      interval=INTERVAL_MONTH
    )
    #import pprint
    #pprint.pprint(results)

    self.assertEqual([(r['start_date'], r['active_site_count']) for r in results], [
      (datetime.date(2019, 1, 1), 0L),
      (datetime.date(2019, 2, 1), 2L),
      (datetime.date(2019, 3, 1), 0L),
      (datetime.date(2019, 4, 1), 1L),
      (datetime.date(2019, 5, 1), 0L),
    ])

  def test_get_active_site_counts_in_interval_quarter(self):
    #
    # create minimum data needed for test
    #
    self._fill_calendar_range(datetime.date(2018, 12, 1), datetime.date(2020, 1, 1))

    site_1 = self.site_dao.get(1)
    site_2 = self.site_dao.get(2)

    participant_1 = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant_1)
    summary_1 = self.participant_summary(participant_1)
    summary_1.siteId = site_1.siteId
    self.participant_summary_dao.insert(summary_1)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 5, 1),
      received_time=datetime.datetime(2019, 5, 2)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 11, 1),
      received_time=datetime.datetime(2019, 11, 2)
    ))
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_1,
      recorded_time=datetime.datetime(2019, 11, 1),
      received_time=datetime.datetime(2019, 11, 2)
    ))
    self.participant_summary_dao.update(summary_1)

    participant_2 = Participant(participantId=2, biobankId=3)
    self.participant_dao.insert(participant_2)
    summary_2 = self.participant_summary(participant_2)
    summary_2.siteId = site_2.siteId
    self.participant_summary_dao.insert(summary_2)
    self.ehr_receipt_dao.insert(self.participant_summary_dao.update_with_new_ehr(
      participant_summary=summary_2,
      recorded_time=datetime.datetime(2019, 5, 1),
      received_time=datetime.datetime(2019, 5, 2)
    ))
    self.participant_summary_dao.update(summary_2)

    #
    # begin test
    #
    results = self.ehr_receipt_dao.get_active_site_counts_in_interval(
      start_date=datetime.datetime(2019, 1, 1),
      end_date=datetime.datetime(2020, 1, 1),
      interval=INTERVAL_QUARTER
    )
    #import pprint
    #pprint.pprint(results)

    self.assertEqual([(r['start_date'], r['active_site_count']) for r in results], [
      (datetime.date(2019, 1, 1), 0L),
      (datetime.date(2019, 4, 1), 2L),
      (datetime.date(2019, 7, 1), 0L),
      (datetime.date(2019, 10, 1), 1L),
      (datetime.date(2020, 1, 1), 0L),
    ])

