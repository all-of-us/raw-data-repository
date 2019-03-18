import csv
import random
import pytz
import datetime
import time
import StringIO

from cloudstorage import cloudstorage_api  # stubbed by testbed

import clock
import config
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from offline import biobank_samples_pipeline
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase
from test import test_data
from model.config_utils import to_client_biobank_id, get_biobank_id_prefix
from model.participant import Participant

from model.biobank_stored_sample import BiobankStoredSample
from model.biobank_dv_order import BiobankDVOrder
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample


from participant_enums import SampleStatus, get_sample_status_enum_value

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = 'rdr_fake_bucket'


class BiobankSamplesPipelineTest(CloudStorageSqlTestBase, NdbTestBase):
  def setUp(self):
    super(BiobankSamplesPipelineTest, self).setUp(use_mysql=True)
    NdbTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, _BASELINE_TESTS)
    # Everything is stored as a list, so override bucket name as a 1-element list.
    config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BUCKET])
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()

  def _write_cloud_csv(self, file_name, contents_str):
    with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
      cloud_file.write(contents_str.encode('utf-8'))

  def _make_biobank_order(self, **kwargs):
    """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
    participantId = kwargs['participantId']

    for k, default_value in (
        ('biobankOrderId', u'1'),
        ('created', clock.CLOCK.now()),
        # ('participantId', self.participant.participantId),
        ('sourceSiteId', 1),
        ('sourceUsername', u'fred@pmi-ops.org'),
        ('collectedSiteId', 1),
        ('collectedUsername', u'joe@pmi-ops.org'),
        ('processedSiteId', 1),
        ('processedUsername', u'sue@pmi-ops.org'),
        ('finalizedSiteId', 2),
        ('finalizedUsername', u'bob@pmi-ops.org'),
        ('version', 1),
        ('identifiers', [BiobankOrderIdentifier(system=u'a', value=u'c')]),
        ('samples', [BiobankOrderedSample(
            test=u'1SAL2',
            description=u'description',
            processingRequired=True)]),
        ('dvOrders', [BiobankDVOrder(
          participantId=participantId
        )])):
      if k not in kwargs:
        kwargs[k] = default_value
    return BiobankOrder(**kwargs)

  def test_dv_order_sample_update(self):
    """
    Test Biobank Direct Volunteer order
    """
    participant = self.participant_dao.insert(Participant())
    self.summary_dao.insert(self.participant_summary(participant))

    created_ts = datetime.datetime(2019, 03, 22, 18, 30, 45)
    confirmed_ts = datetime.datetime(2019, 03, 23, 12, 13, 00)
    disposed_ts = datetime.datetime(2019, 03, 24, 15, 59, 30)

    bo = self._make_biobank_order(participantId=participant.participantId)
    BiobankOrderDao().insert(bo)

    boi = bo.identifiers[0]

    bss = BiobankStoredSample(
      biobankStoredSampleId=u'23523523', biobankId=participant.biobankId, test=u'1SAL2',
      created=created_ts, biobankOrderIdentifier=boi.value)

    with self.participant_dao.session() as session:
      session.add(bss)

    ps = self.summary_dao.get(participant.participantId)
    self.assertIsNone(ps.sampleStatusDV1SAL2)
    self.assertIsNone(ps.sampleStatusDV1SAL2Time)

    self.summary_dao._update_dv_stored_samples()
    ps = self.summary_dao.get(participant.participantId)
    self.assertEqual(ps.sampleStatusDV1SAL2, SampleStatus.RECEIVED)
    self.assertEqual(ps.sampleStatusDV1SAL2Time, created_ts)

    with self.participant_dao.session() as session:
      bss.confirmed = confirmed_ts
      session.add(bss)

    self.summary_dao._update_dv_stored_samples()
    ps = self.summary_dao.get(participant.participantId)
    self.assertEqual(ps.sampleStatusDV1SAL2Time, confirmed_ts)

    with self.participant_dao.session() as session:
      bss.disposed = disposed_ts
      bss.status = SampleStatus.ACCESSINGING_ERROR
      session.add(bss)

    self.summary_dao._update_dv_stored_samples()
    ps = self.summary_dao.get(participant.participantId)
    self.assertEqual(ps.sampleStatusDV1SAL2, SampleStatus.ACCESSINGING_ERROR)
    self.assertEqual(ps.sampleStatusDV1SAL2Time, disposed_ts)



  def test_end_to_end(self):
    dao = BiobankStoredSampleDao()
    self.assertEquals(dao.count(), 0)

    # Create 3 participants and pass their (random) IDs into sample rows.
    summary_dao = ParticipantSummaryDao()
    biobank_ids = []
    participant_ids = []
    nids = 16  # equal to the number of parent rows in 'biobank_samples_1.csv'
    cids = 1   # equal to the number of child rows in 'biobank_samples_1.csv'

    for _ in xrange(nids):
      participant = self.participant_dao.insert(Participant())
      summary_dao.insert(self.participant_summary(participant))
      participant_ids.append(participant.participantId)
      biobank_ids.append(participant.biobankId)
      self.assertEquals(summary_dao.get(participant.participantId).numBaselineSamplesArrived, 0)

    test_codes = random.sample(_BASELINE_TESTS, nids)
    samples_file = test_data.open_biobank_samples(biobank_ids=biobank_ids, tests=test_codes)
    lines = samples_file.split('\n')[1:] # remove field name line

    input_filename = 'cloud%s.csv' % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
        biobank_samples_pipeline.INPUT_CSV_TIME_FORMAT)
    self._write_cloud_csv(input_filename, samples_file)
    biobank_samples_pipeline.upsert_from_latest_csv()

    self.assertEquals(dao.count(), nids - cids)

    for x in range(0, nids):
      cols = lines[x].split('\t')

      if cols[10].strip():  # skip child sample
        continue

      # If status is 'In Prep', then sample confirmed timestamp should be empty
      if cols[2] == 'In Prep':
        self.assertEquals(len(cols[11]), 0)
      else:
        status = SampleStatus.RECEIVED
        ts_str = cols[11]
        # DA-814 - Participant Summary test status should be: Unset, Received or Disposed only.
        # If sample is disposed, then check disposed timestamp, otherwise check confirmed timestamp.
        # DA-871 - Only check status is disposed when reason code is a bad disposal.
        if cols[2] == 'Disposed' and get_sample_status_enum_value(cols[8]) > SampleStatus.UNKNOWN:
          status = SampleStatus.DISPOSED
          ts_str = cols[9]

        ts = datetime.datetime.strptime(ts_str, '%Y/%m/%d %H:%M:%S')
        self._check_summary(participant_ids[x], test_codes[x], ts, status)

  def test_old_csv_not_imported(self):
    now = clock.CLOCK.now()
    too_old_time = now - datetime.timedelta(hours=25)
    input_filename = 'cloud%s.csv' % self._naive_utc_to_naive_central(too_old_time).strftime(
        biobank_samples_pipeline.INPUT_CSV_TIME_FORMAT)
    self._write_cloud_csv(input_filename, '')
    with self.assertRaises(biobank_samples_pipeline.DataError):
      biobank_samples_pipeline.upsert_from_latest_csv()

  def _naive_utc_to_naive_central(self, naive_utc_date):
    utc_date = pytz.utc.localize(naive_utc_date)
    central_date = utc_date.astimezone(pytz.timezone('US/Central'))
    return central_date.replace(tzinfo=None)

  def _check_summary(self, participant_id, test, date_formatted, status):
    summary = ParticipantSummaryDao().get(participant_id)
    self.assertEquals(summary.numBaselineSamplesArrived, 1)
    # DA-614 - All specific disposal statuses in biobank_stored_samples are changed to DISPOSED
    # in the participant summary.
    self.assertEquals(status, getattr(summary, 'sampleStatus' + test))
    sample_time = self._naive_utc_to_naive_central(getattr(summary, 'sampleStatus' + test + 'Time'))
    self.assertEquals(date_formatted, sample_time)

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
    samples_file = test_data.open_biobank_samples([112, 222, 333], [])
    reader = csv.DictReader(StringIO.StringIO(samples_file), delimiter='\t')
    row = reader.next()
    sample = biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())
    self.assertIsNotNone(sample)

    cols = biobank_samples_pipeline.CsvColumns
    self.assertEquals(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
    self.assertEquals(to_client_biobank_id(sample.biobankId), row[cols.EXTERNAL_PARTICIPANT_ID])
    self.assertEquals(sample.test, row[cols.TEST_CODE])
    confirmed_date = self._naive_utc_to_naive_central(sample.confirmed)
    self.assertEquals(
        confirmed_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT),
        row[cols.CONFIRMED_DATE])
    received_date = self._naive_utc_to_naive_central(sample.created)
    self.assertEquals(
        received_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT),
        row[cols.CREATE_DATE])

  def test_sample_from_row_wrong_prefix(self):
    samples_file = test_data.open_biobank_samples([111, 222, 333], [])
    reader = csv.DictReader(StringIO.StringIO(samples_file), delimiter='\t')
    row = reader.next()
    row[biobank_samples_pipeline.CsvColumns.CONFIRMED_DATE] = '2016 11 19'
    self.assertIsNone(biobank_samples_pipeline._create_sample_from_row(row, 'Q'))

  def test_sample_from_row_invalid(self):
    samples_file = test_data.open_biobank_samples([111, 222, 333], [])
    reader = csv.DictReader(StringIO.StringIO(samples_file), delimiter='\t')
    row = reader.next()
    row[biobank_samples_pipeline.CsvColumns.CONFIRMED_DATE] = '2016 11 19'
    with self.assertRaises(biobank_samples_pipeline.DataError):
      biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())

  def test_sample_from_row_old_test(self):
    samples_file = test_data.open_biobank_samples([111, 222, 333], [])
    reader = csv.DictReader(StringIO.StringIO(samples_file), delimiter='\t')
    row = reader.next()
    row[biobank_samples_pipeline.CsvColumns.TEST_CODE] = '2PST8'
    sample = biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())
    self.assertIsNotNone(sample)
    cols = biobank_samples_pipeline.CsvColumns
    self.assertEquals(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
    self.assertEquals(sample.test, row[cols.TEST_CODE])

  def test_column_missing(self):
    with open(test_data.data_path('biobank_samples_missing_field.csv')) as samples_file:
      reader = csv.DictReader(samples_file, delimiter='\t')
      with self.assertRaises(biobank_samples_pipeline.DataError):
        biobank_samples_pipeline._upsert_samples_from_csv(reader)


  def test_get_reconciliation_report_paths(self):
    dt = datetime.datetime(2016, 12, 22, 18, 30, 45)
    expected_prefix = 'reconciliation/report_2016-12-22'
    paths = biobank_samples_pipeline._get_report_paths(dt)
    self.assertEquals(len(paths), 4)
    for path in paths:
      self.assertTrue(
          path.startswith(expected_prefix),
          'Report path %r must start with %r.' % (expected_prefix, path))
      self.assertTrue(path.endswith('.csv'))
