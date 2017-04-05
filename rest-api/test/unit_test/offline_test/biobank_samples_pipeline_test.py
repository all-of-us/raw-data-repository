import csv
import random
import pytz
import time

from cloudstorage import cloudstorage_api  # stubbed by testbed

import config
from code_constants import BIOBANK_TESTS
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.hpo_dao import HPO
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from offline import biobank_samples_pipeline
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase
from test import test_data
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

  def _write_cloud_csv(self, file_name, contents_str):
    with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
      cloud_file.write(contents_str)

  def test_end_to_end(self):
    dao = BiobankStoredSampleDao()
    self.assertEquals(dao.count(), 0)

    # Create 3 participants and pass their (random) IDs into sample rows.
    participant_dao = ParticipantDao()
    summary_dao = ParticipantSummaryDao()
    biobank_ids = []
    participant_ids = []
    for _ in xrange(3):
      participant = participant_dao.insert(Participant())
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
    self._write_cloud_csv('c_lex_last_created_middle.csv', 'any contents')
    time.sleep(1.0)
    created_last = 'b_lex_middle_created_last.csv'
    self._write_cloud_csv(created_last, 'any contents')

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


# TODO(mwf) Add Biobank reconciliation test using this stub.
class MySqlReconciliationTest(CloudStorageSqlTestBase, NdbTestBase):
  def setUp(self):
    super(MySqlReconciliationTest, self).setUp(use_mysql=True, with_data=False)

  def _create_hpo_as_sanity_check(self):
    session = self.database.make_session()
    hpo = HPO(hpoId=1, name='UNSET')
    session.add(hpo)
    session.commit()
    session.close()

  def test_mysql_db_connection_works(self):
    self._create_hpo_as_sanity_check()

  def test_mysql_db_connection_works_after_reset(self):
    self._create_hpo_as_sanity_check()
