import csv
import pytz
import datetime
import time

import clock
from code_constants import BIOBANK_TESTS
from dao import database_utils
from dao.biobank_order_dao import BiobankOrderDao
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from offline import biobank_samples_pipeline
from test.unit_test.unit_test_util import SqlTestBase, InMemorySqlExporter, TestBase
from model.biobank_order import BiobankOrder, BiobankOrderedSample
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import to_client_biobank_id
from model.participant import Participant


# Expected names for the reconciliation_data columns in output CSVs.
_CSV_COLUMN_NAMES = (
  'biobank_id',

  'sent_test',
  'sent_count',
  'sent_order_id',
  'sent_collection_time',
  'sent_finalized_time',
  'site_id',

  'received_test',
  'received_count',
  'received_sample_id',
  'received_time',

  'elapsed_hours',
)


class MySqlReconciliationTest(SqlTestBase):
  """Biobank samples pipeline tests requiring slower MySQL (not SQLite)."""
  def setUp(self):
    super(MySqlReconciliationTest, self).setUp(use_mysql=True)
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.order_dao = BiobankOrderDao()
    self.sample_dao = BiobankStoredSampleDao()

  def _insert_participant(self):
    participant = self.participant_dao.insert(Participant())
    # satisfies the consent requirement
    self.summary_dao.insert(self.participant_summary(participant))
    return participant

  def _insert_order(self, participant, order_id, tests, order_time):
    order = BiobankOrder(
        biobankOrderId=order_id,
        participantId=participant.participantId,
        sourceSiteValue='SiteValue-%s' % participant.participantId,
        created=order_time,
        samples=[])
    for test_code in tests:
      order.samples.append(BiobankOrderedSample(
          biobankOrderId=order.biobankOrderId,
          test=test_code,
          description=u'test',
          processingRequired=False,
          collected=order_time,
          finalized=order_time))
    return self.order_dao.insert(order)

  def _insert_samples(self, participant, tests, sample_ids, received_time):
    for test_code, sample_id in zip(tests, sample_ids):
      self.sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId=sample_id,
          biobankId=participant.biobankId,
          test=test_code,
          confirmed=received_time))

  def test_reconciliation_query(self):
    # MySQL and Python sub-second rounding differs, so trim micros from generated times.
    order_time = clock.CLOCK.now().replace(microsecond=0)
    within_a_day = order_time + datetime.timedelta(hours=23)
    late_time = order_time + datetime.timedelta(hours=25)

    p_on_time = self._insert_participant()
    self._insert_order(p_on_time, 'GoodOrder', BIOBANK_TESTS[:2], order_time)
    self._insert_samples(p_on_time, BIOBANK_TESTS[:2], ['GoodSample1', 'GoodSample2'], within_a_day)

    p_late_and_missing = self._insert_participant()
    o_late_and_missing = self._insert_order(
        p_late_and_missing, 'SlowOrder', BIOBANK_TESTS[:2], order_time)
    self._insert_samples(p_late_and_missing, [BIOBANK_TESTS[0]], ['LateSample'], late_time)

    p_extra = self._insert_participant()
    self._insert_samples(p_extra, [BIOBANK_TESTS[-1]], ['NobodyOrderedThisSample'], order_time)

    # for the same participant/test, 3 orders sent and only 2 samples received.
    p_repeated = self._insert_participant()
    for repetition in xrange(3):
      self._insert_order(
          p_repeated,
          'RepeatedOrder%d' % repetition,
          [BIOBANK_TESTS[0]],
          order_time + datetime.timedelta(weeks=repetition))
      if repetition != 2:
        self._insert_samples(
            p_repeated,
            [BIOBANK_TESTS[0]],
            ['RepeatedSample%d' % repetition],
            within_a_day + datetime.timedelta(weeks=repetition))

    received, late, missing = 'rx.csv', 'late.csv', 'missing.csv'
    exporter = InMemorySqlExporter(self)
    biobank_samples_pipeline._query_and_write_reports(exporter, received, late, missing)

    exporter.assertFilesEqual((received, late, missing))

    # sent-and-received: 2 on-time, 1 late, none of the missing/extra/repeated ones
    exporter.assertRowCount(received, 3)
    exporter.assertColumnNamesEqual(received, _CSV_COLUMN_NAMES)
    row = exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId),
        'sent_test': BIOBANK_TESTS[0],
        'received_test': BIOBANK_TESTS[0]})
    # Also check the values of all remaining fields on one row.
    self.assertEquals(row['site_id'], 'SiteValue-%d' % p_on_time.participantId)
    self.assertEquals(row['sent_finalized_time'], database_utils.format_datetime(order_time))
    self.assertEquals(row['sent_collection_time'], database_utils.format_datetime(order_time))
    self.assertEquals(row['received_time'], database_utils.format_datetime(within_a_day))
    self.assertEquals(row['sent_count'], '1')
    self.assertEquals(row['received_count'], '1')
    self.assertEquals(row['sent_order_id'], 'GoodOrder')
    self.assertEquals(row['received_sample_id'], 'GoodSample1')
    # the other sent-and-received rows
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId), 'sent_test': BIOBANK_TESTS[1]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_test': BIOBANK_TESTS[0]})

    # sent-and-received: 1 late
    exporter.assertRowCount(late, 1)
    exporter.assertColumnNamesEqual(late, _CSV_COLUMN_NAMES)
    exporter.assertHasRow(late, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_order_id': o_late_and_missing.biobankOrderId,
        'elapsed_hours': '25'})

    # orders/samples where something went wrong
    exporter.assertRowCount(missing, 3)
    exporter.assertColumnNamesEqual(missing, _CSV_COLUMN_NAMES)
    # order sent, no sample received
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_order_id': o_late_and_missing.biobankOrderId,
        'elapsed_hours': ''})
    # sample received, nothing ordered
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_extra.biobankId), 'sent_order_id': ''})
    # 3 orders sent, only 2 received
    multi_sample_row = exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_repeated.biobankId),
        'sent_count': '3',
        'received_count': '2'})
    # Also verify the comma-joined fields of the row with multiple orders/samples.
    self.assertItemsEqual(
        multi_sample_row['sent_order_id'].split(','),
        ['RepeatedOrder1', 'RepeatedOrder0', 'RepeatedOrder2'])
    self.assertItemsEqual(
        multi_sample_row['received_sample_id'].split(','),
        ['RepeatedSample0', 'RepeatedSample1'])
