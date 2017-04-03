import csv
import random
import pytz
import datetime
import time

from cloudstorage import cloudstorage_api  # stubbed by testbed

import clock
import config
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from offline import biobank_samples_pipeline
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, InMemorySqlExporter
from test.unit_test.unit_test_util import SqlTestBase, InMemorySqlExporter, TestBase
from test import test_data
from model.biobank_order import BiobankOrder, BiobankOrderedSample
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import to_client_biobank_id
from model.participant import Participant
from participant_enums import SampleStatus

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = 'rdr_fake_bucket'


class BiobankSamplesPipelineTest(CloudStorageSqlTestBase, NdbTestBase):
  def setUp(self):
    super(BiobankSamplesPipelineTest, self).setUp()
    NdbTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, _BASELINE_TESTS)
    # Everything is stored as a list, so override bucket name as a 1-element list.
    config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BUCKET])
    self.participant_dao = ParticipantDao()

  def _write_cloud_csv(self, file_name, contents_str):
    with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
      cloud_file.write(contents_str)

  def test_end_to_end(self):
    dao = BiobankStoredSampleDao()
    self.assertEquals(dao.count(), 0)

    # Create 3 participants and pass their (random) IDs into sample rows.
    summary_dao = ParticipantSummaryDao()
    biobank_ids = []
    participant_ids = []
    for _ in xrange(3):
      participant = self.participant_dao.insert(Participant())
      summary_dao.insert(self.participant_summary(participant))
      participant_ids.append(participant.participantId)
      biobank_ids.append(participant.biobankId)
      self.assertEquals(summary_dao.get(participant.participantId).numBaselineSamplesArrived, 0)
    test1, test2, test3 = random.sample(_BASELINE_TESTS, 3)
    samples_file = test_data.open_biobank_samples(*biobank_ids, test1=test1, test2=test2,
                                                  test3=test3)
    self._write_cloud_csv('cloud.csv', samples_file.read())

    biobank_samples_pipeline.upsert_from_latest_csv()

    self.assertEquals(dao.count(), 3)
    self._check_summary(participant_ids[0], test1, '2016-11-29T12:19:32')
    self._check_summary(participant_ids[1], test2, '2016-11-29T12:38:58')
    self._check_summary(participant_ids[2], test3, '2016-11-29T12:41:26')

  def _naive_utc_to_naive_central(self, naive_utc_date):
    utc_date = pytz.utc.localize(naive_utc_date)
    central_date = utc_date.astimezone(pytz.timezone('US/Central'))
    return central_date.replace(tzinfo=None)

  def _check_summary(self, participant_id, test, date_formatted):
    summary = ParticipantSummaryDao().get(participant_id)
    self.assertEquals(summary.numBaselineSamplesArrived, 1)
    self.assertEquals(SampleStatus.RECEIVED, getattr(summary, 'sampleStatus' + test))
    sample_time = self._naive_utc_to_naive_central(getattr(summary, 'sampleStatus' + test + 'Time'))
    self.assertEquals(date_formatted, sample_time.isoformat())

  def test_find_latest_csv(self):
    # The cloud storage testbed does not expose an injectable time function.
    # Creation time is stored at second granularity.
    self._write_cloud_csv('a_lex_first_created_first.csv', 'any contents')
    time.sleep(1.0)
    self._write_cloud_csv('z_lex_last_created_middle.csv', 'any contents')
    time.sleep(1.0)
    created_last = 'b_lex_middle_created_last.csv'
    self._write_cloud_csv(created_last, 'any contents')
    self._write_cloud_csv(
        '%s/created_last_in_subdir.csv' % biobank_samples_pipeline._REPORT_SUBDIR, 'any contents')

    latest_filename = biobank_samples_pipeline._find_latest_samples_csv(_FAKE_BUCKET)
    self.assertEquals(latest_filename, '/%s/%s' % (_FAKE_BUCKET, created_last))

  def test_sample_from_row(self):
    samples_file = test_data.open_biobank_samples(111, 222, 333)
    reader = csv.DictReader(samples_file, delimiter='\t')
    row = reader.next()

    sample = biobank_samples_pipeline._create_sample_from_row(row)
    self.assertIsNotNone(sample)

    cols = biobank_samples_pipeline._Columns
    self.assertEquals(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
    self.assertEquals(to_client_biobank_id(sample.biobankId), row[cols.EXTERNAL_PARTICIPANT_ID])
    self.assertEquals(sample.test, row[cols.TEST_CODE])
    confirmed_date = self._naive_utc_to_naive_central(sample.confirmed)
    self.assertEquals(
        confirmed_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT),
        row[cols.CONFIRMED_DATE])

  def test_column_missing(self):
    with open(test_data.data_path('biobank_samples_missing_field.csv')) as samples_file:
      reader = csv.DictReader(samples_file, delimiter='\t')
      with self.assertRaises(RuntimeError):
        biobank_samples_pipeline._upsert_samples_from_csv(reader)

  def test_get_reconciliation_report_paths(self):
    dt = datetime.datetime(2016, 12, 22, 18, 30, 45)
    expected_prefix = 'reconciliation/report_2016-12-22'
    paths = biobank_samples_pipeline._get_report_paths(dt)
    self.assertEquals(len(paths), 3)
    for path in paths:
      self.assertTrue(
          path.startswith(expected_prefix),
          'Report path %r must start with %r.' % (expected_prefix, path))
      self.assertTrue(path.endswith('.csv'))


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
    self._insert_order(p_on_time, 'GoodOrder', _BASELINE_TESTS[:2], order_time)
    self._insert_samples(p_on_time, _BASELINE_TESTS[:2], within_a_day)

    p_late_and_missing = self._insert_participant()
    o_late_and_missing = self._insert_order(
        p_late_and_missing, 'SlowOrder', _BASELINE_TESTS[:2], order_time)
    self._insert_samples(p_late_and_missing, [_BASELINE_TESTS[0]], late_time)

    p_extra = self._insert_participant()
    self._insert_samples(p_extra, [_BASELINE_TESTS[-1]], order_time)

    p_repeated = self._insert_participant()
    self._insert_order(p_repeated, 'OrigOrder', [_BASELINE_TESTS[0]], order_time)
    self._insert_samples(p_repeated, [_BASELINE_TESTS[0]], within_a_day)
    self._insert_order(p_repeated, 'RepeatedOrder', [_BASELINE_TESTS[0]], days_later)

    received, late, missing = 'rx.csv', 'late.csv', 'missing.csv'
    exporter = InMemorySqlExporter(self)
    biobank_samples_pipeline._query_and_write_reports(exporter, received, late, missing)

    exporter.assertFilesEqual((received, late, missing))

    # sent-and-received: 2 on-time, 1 late, none of the missing/extra/repeated ones
    exporter.assertRowCount(received, 3)
    exporter.assertColumnNamesEqual(received, _CSV_COLUMN_NAMES)
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId), 'sent_test': _BASELINE_TESTS[0]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId), 'sent_test': _BASELINE_TESTS[1]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_test': _BASELINE_TESTS[0]})

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
