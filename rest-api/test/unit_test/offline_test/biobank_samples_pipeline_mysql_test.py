import csv
import pytz
import datetime
import time

import clock
from code_constants import BIOBANK_TESTS
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

  def _insert_samples(self, participant, tests, received_time):
    for test_code in tests:
      self.sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId='StoredSample-%s-%s' % (participant.participantId, test_code),
          biobankId=participant.biobankId,
          test=test_code,
          confirmed=received_time))

  def test_reconciliation_query(self):
    order_time = clock.CLOCK.now()
    within_a_day = order_time + datetime.timedelta(hours=23)
    late_time = order_time + datetime.timedelta(hours=25)
    days_later = order_time + datetime.timedelta(days=5)

    p_on_time = self._insert_participant()
    self._insert_order(p_on_time, 'GoodOrder', BIOBANK_TESTS[:2], order_time)
    self._insert_samples(p_on_time, BIOBANK_TESTS[:2], within_a_day)

    p_late_and_missing = self._insert_participant()
    o_late_and_missing = self._insert_order(
        p_late_and_missing, 'SlowOrder', BIOBANK_TESTS[:2], order_time)
    self._insert_samples(p_late_and_missing, [BIOBANK_TESTS[0]], late_time)

    p_extra = self._insert_participant()
    self._insert_samples(p_extra, [BIOBANK_TESTS[-1]], order_time)

    p_repeated = self._insert_participant()
    self._insert_order(p_repeated, 'OrigOrder', [BIOBANK_TESTS[0]], order_time)
    self._insert_samples(p_repeated, [BIOBANK_TESTS[0]], within_a_day)
    self._insert_order(p_repeated, 'RepeatedOrder', [BIOBANK_TESTS[0]], days_later)

    received, late, missing = 'rx.csv', 'late.csv', 'missing.csv'
    exporter = InMemorySqlExporter(self)
    biobank_samples_pipeline._query_and_write_reports(exporter, received, late, missing)

    exporter.assertFilesEqual((received, late, missing))

    # sent-and-received: 2 on-time, 1 late, none of the missing/extra/repeated ones
    exporter.assertRowCount(received, 3)
    exporter.assertColumnNamesEqual(received, _CSV_COLUMN_NAMES)
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId), 'sent_test': BIOBANK_TESTS[0]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId), 'sent_test': BIOBANK_TESTS[1]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_test': BIOBANK_TESTS[0]})

    # sent-and-received: 1 late
    exporter.assertRowCount(late, 1)
    exporter.assertHasRow(late, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_order_id': o_late_and_missing.biobankOrderId,
        'elapsed_hours': '25'})

    # gone awry
    exporter.assertRowCount(missing, 3)
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_order_id': o_late_and_missing.biobankOrderId,
        'elapsed_hours': ''})
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_repeated.biobankId),
        'sent_count': '2',
        'received_count': '1'})
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_extra.biobankId), 'sent_order_id': ''})
