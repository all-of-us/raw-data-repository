import datetime
import clock
import config
import pytz
import csv
from cloudstorage import cloudstorage_api  # stubbed by testbed
from code_constants import BIOBANK_TESTS
from model.participant import Participant
from genomic import genomic_set_file_handler
from test import test_data
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase
from dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.biobank_dv_order import BiobankDVOrder
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus, GenomicSetMemberStatus
from offline import genomic_pipeline
from participant_enums import SampleStatus
from genomic.genomic_set_file_handler import DataError

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = 'rdr_fake_bucket'
_FAKE_BIOBANK_SAMPLE_BUCKET = 'rdr_fake_biobank_sample_bucket'
_FAKE_BUCKET_FOLDER = 'rdr_fake_sub_folder'
_FAKE_BUCKET_RESULT_FOLDER = 'rdr_fake_sub_result_folder'
_OUTPUT_CSV_TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
_US_CENTRAL = pytz.timezone('US/Central')
_UTC = pytz.utc

class GenomicPipelineTest(CloudStorageSqlTestBase, NdbTestBase):
  def setUp(self):
    super(GenomicPipelineTest, self).setUp(use_mysql=True)
    NdbTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    # Everything is stored as a list, so override bucket name as a 1-element list.
    config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])
    config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BIOBANK_SAMPLE_BUCKET])
    config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME, [_FAKE_BUCKET_FOLDER])
    config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME,
                            [_FAKE_BUCKET_RESULT_FOLDER])
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self._participant_i = 1

  def _write_cloud_csv(self, file_name, contents_str, bucket=None, folder=None):
    bucket = _FAKE_BUCKET if bucket is None else bucket
    if folder is None:
      path = '/%s/%s' % (bucket, file_name)
    else:
      path = '/%s/%s/%s' % (bucket, folder, file_name)
    with cloudstorage_api.open(path, mode='w') as cloud_file:
      cloud_file.write(contents_str.encode('utf-8'))

  def _make_participant(self, **kwargs):
    """
    Make a participant with custom settings.
    default should create a valid participant.
    """
    i = self._participant_i
    self._participant_i += 1
    participant = Participant(
      participantId = i,
      biobankId = i,
      **kwargs
    )
    self.participant_dao.insert(participant)
    return participant

  def _make_biobank_order(self, **kwargs):
    """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
    participant_id = kwargs['participantId']

    for k, default_value in (
        ('biobankOrderId', u'1'),
        ('created', clock.CLOCK.now()),
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
          participantId=participant_id, version=1)])):
      if k not in kwargs:
        kwargs[k] = default_value

    biobank_order = BiobankOrderDao().insert(BiobankOrder(**kwargs))
    return biobank_order

  def _make_summary(self, participant, **override_kwargs):
    """
    Make a summary with custom settings.
    default should create a valid summary.
    """
    valid_kwargs = dict(
      participantId = participant.participantId,
      biobankId=participant.biobankId,
      withdrawalStatus=participant.withdrawalStatus,
      dateOfBirth=datetime.datetime(2000, 1, 1),
      firstName='foo',
      lastName='bar',
      zipCode='12345',
      sampleStatus1ED04=SampleStatus.RECEIVED,
      sampleStatus1SAL2=SampleStatus.RECEIVED,
      samplesToIsolateDNA=SampleStatus.RECEIVED,
      consentForStudyEnrollmentTime=datetime.datetime(2019, 1, 1)
    )
    kwargs = dict(valid_kwargs, **override_kwargs)
    summary = self._participant_summary_with_defaults(**kwargs)
    self.summary_dao.insert(summary)
    return summary

  def test_end_to_end_valid_case(self):
    participant = self._make_participant()
    self._make_summary(participant)
    self._make_biobank_order(participantId=participant.participantId,
                             biobankOrderId=participant.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'https://www.pmi-ops.org',
                                                                 value=u'12345678')])

    participant2 = self._make_participant()
    self._make_summary(participant2)
    self._make_biobank_order(participantId=participant2.participantId,
                             biobankOrderId=participant2.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'https://www.pmi-ops.org',
                                                                 value=u'12345679')])

    participant3 = self._make_participant()
    self._make_summary(participant3)
    self._make_biobank_order(participantId=participant3.participantId,
                             biobankOrderId=participant3.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'https://www.pmi-ops.org',
                                                                 value=u'12345680')])

    samples_file = test_data.open_genomic_set_file('Genomic-Test-Set-test-2.csv')

    input_filename = 'Genomic-Test-Set-v1%s.csv' % self\
      ._naive_utc_to_naive_central(clock.CLOCK.now())\
      .strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(input_filename, samples_file)

    manifest_result_file = test_data.open_genomic_set_file('Genomic-Manifest-Result-test.csv')

    manifest_result_filename = 'Genomic-Manifest-Result-AoU-1-v1%s.csv' % self \
      ._naive_utc_to_naive_central(clock.CLOCK.now()) \
      .strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(manifest_result_filename, manifest_result_file, bucket=_FAKE_BIOBANK_SAMPLE_BUCKET,
                          folder=_FAKE_BUCKET_RESULT_FOLDER)

    genomic_pipeline.process_genomic_water_line()

    # verify result file
    bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
    path = self._find_latest_genomic_set_csv(bucket_name, 'Validation-Result')
    csv_file = cloudstorage_api.open(path)
    csv_reader = csv.DictReader(csv_file, delimiter=',')

    class ResultCsvColumns(object):
      """Names of CSV columns that we read from the genomic set upload."""
      GENOMIC_SET_NAME = 'genomic_set_name'
      GENOMIC_SET_CRITERIA = 'genomic_set_criteria'
      PID = 'pid'
      BIOBANK_ORDER_ID = 'biobank_order_id'
      NY_FLAG = 'ny_flag'
      SEX_AT_BIRTH = 'sex_at_birth'
      GENOME_TYPE = 'genome_type'
      STATUS = 'status'
      INVALID_REASON = 'invalid_reason'

      ALL = (GENOMIC_SET_NAME, GENOMIC_SET_CRITERIA, PID, BIOBANK_ORDER_ID, NY_FLAG, SEX_AT_BIRTH,
             GENOME_TYPE, STATUS, INVALID_REASON)

    missing_cols = set(ResultCsvColumns.ALL) - set(csv_reader.fieldnames)
    self.assertEqual(len(missing_cols), 0)
    rows = list(csv_reader)
    self.assertEqual(len(rows), 3)
    self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_NAME], 'name_xxx')
    self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'criteria_xxx')
    self.assertEqual(rows[0][ResultCsvColumns.STATUS], 'valid')
    self.assertEqual(rows[0][ResultCsvColumns.INVALID_REASON], '')
    self.assertEqual(rows[0][ResultCsvColumns.PID], '1')
    self.assertEqual(rows[0][ResultCsvColumns.BIOBANK_ORDER_ID], '1')
    self.assertEqual(rows[0][ResultCsvColumns.NY_FLAG], 'Y')
    self.assertEqual(rows[0][ResultCsvColumns.GENOME_TYPE], 'aou_wgs')
    self.assertEqual(rows[0][ResultCsvColumns.SEX_AT_BIRTH], 'M')

    self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_NAME], 'name_xxx')
    self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'criteria_xxx')
    self.assertEqual(rows[1][ResultCsvColumns.STATUS], 'valid')
    self.assertEqual(rows[1][ResultCsvColumns.INVALID_REASON], '')
    self.assertEqual(rows[1][ResultCsvColumns.PID], '2')
    self.assertEqual(rows[1][ResultCsvColumns.BIOBANK_ORDER_ID], '2')
    self.assertEqual(rows[1][ResultCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[1][ResultCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[1][ResultCsvColumns.SEX_AT_BIRTH], 'F')

    self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_NAME], 'name_xxx')
    self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'criteria_xxx')
    self.assertEqual(rows[2][ResultCsvColumns.STATUS], 'valid')
    self.assertEqual(rows[2][ResultCsvColumns.INVALID_REASON], '')
    self.assertEqual(rows[2][ResultCsvColumns.PID], '3')
    self.assertEqual(rows[2][ResultCsvColumns.BIOBANK_ORDER_ID], '3')
    self.assertEqual(rows[2][ResultCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[2][ResultCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[2][ResultCsvColumns.SEX_AT_BIRTH], 'M')

    # verify manifest files
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

    class ExpectedCsvColumns(object):
      VALUE = 'value'
      BIOBANK_ID = 'biobank_id'
      SEX_AT_BIRTH = 'sex_at_birth'
      GENOME_TYPE = 'genome_type'
      NY_FLAG = 'ny_flag'
      REQUEST_ID = 'request_id'
      PACKAGE_ID = 'package_id'

      ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG, REQUEST_ID, PACKAGE_ID)

    path = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)

    csv_file = cloudstorage_api.open(path)
    csv_reader = csv.DictReader(csv_file, delimiter=',')

    missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
    self.assertEqual(len(missing_cols), 0)
    rows = list(csv_reader)
    self.assertEqual(rows[0][ExpectedCsvColumns.VALUE], '')
    self.assertEqual(rows[0][ExpectedCsvColumns.BIOBANK_ID], 'T1')
    self.assertEqual(rows[0][ExpectedCsvColumns.SEX_AT_BIRTH], 'M')
    self.assertEqual(rows[0][ExpectedCsvColumns.GENOME_TYPE], 'aou_wgs')
    self.assertEqual(rows[0][ExpectedCsvColumns.NY_FLAG], 'Y')
    self.assertEqual(rows[1][ExpectedCsvColumns.VALUE], '')
    self.assertEqual(rows[1][ExpectedCsvColumns.BIOBANK_ID], 'T2')
    self.assertEqual(rows[1][ExpectedCsvColumns.SEX_AT_BIRTH], 'F')
    self.assertEqual(rows[1][ExpectedCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[1][ExpectedCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[2][ExpectedCsvColumns.VALUE], '')
    self.assertEqual(rows[2][ExpectedCsvColumns.BIOBANK_ID], 'T3')
    self.assertEqual(rows[2][ExpectedCsvColumns.SEX_AT_BIRTH], 'M')
    self.assertEqual(rows[2][ExpectedCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[2][ExpectedCsvColumns.NY_FLAG], 'N')

    # verify manifest result files
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

    class ExpectedCsvColumns(object):
      VALUE = 'value'
      BIOBANK_ID = 'biobank_id'
      SEX_AT_BIRTH = 'sex_at_birth'
      GENOME_TYPE = 'genome_type'
      NY_FLAG = 'ny_flag'
      REQUEST_ID = 'request_id'
      PACKAGE_ID = 'package_id'

      ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG, REQUEST_ID, PACKAGE_ID)

    path = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_RESULT_FOLDER)
    csv_file = cloudstorage_api.open(path)
    csv_reader = csv.DictReader(csv_file, delimiter=',')

    missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
    self.assertEqual(len(missing_cols), 0)
    rows = list(csv_reader)
    self.assertEqual(rows[0][ExpectedCsvColumns.VALUE], '12345678')
    self.assertEqual(rows[0][ExpectedCsvColumns.BIOBANK_ID], 'T1')
    self.assertEqual(rows[0][ExpectedCsvColumns.SEX_AT_BIRTH], 'M')
    self.assertEqual(rows[0][ExpectedCsvColumns.GENOME_TYPE], 'aou_wgs')
    self.assertEqual(rows[0][ExpectedCsvColumns.NY_FLAG], 'Y')
    self.assertEqual(rows[0][ExpectedCsvColumns.PACKAGE_ID], 'PKG-XXXX-XXXX1')

    self.assertEqual(rows[1][ExpectedCsvColumns.VALUE], '12345679')
    self.assertEqual(rows[1][ExpectedCsvColumns.BIOBANK_ID], 'T2')
    self.assertEqual(rows[1][ExpectedCsvColumns.SEX_AT_BIRTH], 'F')
    self.assertEqual(rows[1][ExpectedCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[1][ExpectedCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[1][ExpectedCsvColumns.PACKAGE_ID], 'PKG-XXXX-XXXX2')

    self.assertEqual(rows[2][ExpectedCsvColumns.VALUE], '12345680')
    self.assertEqual(rows[2][ExpectedCsvColumns.BIOBANK_ID], 'T3')
    self.assertEqual(rows[2][ExpectedCsvColumns.SEX_AT_BIRTH], 'M')
    self.assertEqual(rows[2][ExpectedCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[2][ExpectedCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[2][ExpectedCsvColumns.PACKAGE_ID], 'PKG-XXXX-XXXX3')

    # verify package id in database
    member_dao = GenomicSetMemberDao()
    members = member_dao.get_all()
    for member in members:
      self.assertIn(member.packageId, ['PKG-XXXX-XXXX1', 'PKG-XXXX-XXXX2', 'PKG-XXXX-XXXX3'])
      self.assertIn(member.biobankOrderClientId, ['12345678', '12345679', '12345680'])

  def test_wrong_file_name_case(self):
    samples_file = test_data.open_genomic_set_file('Genomic-Test-Set-test-3.csv')

    input_filename = 'Genomic-Test-Set-v1%swrong-name.csv' % self \
      ._naive_utc_to_naive_central(clock.CLOCK.now()) \
      .strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(input_filename, samples_file)

    with self.assertRaises(DataError):
      genomic_pipeline.process_genomic_water_line()

    manifest_result_file = test_data.open_genomic_set_file('Genomic-Manifest-Result-test.csv')

    manifest_result_filename = 'Genomic-Manifest-Result-AoU-1-v1%swrong-name.csv' % self \
      ._naive_utc_to_naive_central(clock.CLOCK.now()) \
      .strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(manifest_result_filename, manifest_result_file, bucket=_FAKE_BIOBANK_SAMPLE_BUCKET,
                          folder=_FAKE_BUCKET_RESULT_FOLDER)

    with self.assertRaises(DataError):
      genomic_pipeline.process_genomic_water_line()

  def test_over_24hours_genomic_set_file_case(self):
    samples_file = test_data.open_genomic_set_file('Genomic-Test-Set-test-3.csv')

    over_24hours_time = clock.CLOCK.now() - datetime.timedelta(hours=25)

    input_filename = 'Genomic-Test-Set-v1%s.csv' % self \
      ._naive_utc_to_naive_central(over_24hours_time) \
      .strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(input_filename, samples_file)

    genomic_pipeline.process_genomic_water_line()

    member_dao = GenomicSetMemberDao()
    members = member_dao.get_all()
    self.assertEqual(len(members), 0)

  def test_end_to_end_invalid_case(self):
    participant = self._make_participant()
    self._make_summary(participant, dateOfBirth='2018-02-14', zipCode='')
    self._make_biobank_order(participantId=participant.participantId,
                             biobankOrderId=participant.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'https://www.pmi-ops.org',
                                                                 value=u'12345678')])

    participant2 = self._make_participant()
    self._make_summary(participant2, consentForStudyEnrollmentTime=datetime.datetime(1990, 1, 1))
    self._make_biobank_order(participantId=participant2.participantId,
                             biobankOrderId=participant2.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'https://www.pmi-ops.org',
                                                                 value=u'12345679')])

    participant3 = self._make_participant()
    self._make_summary(participant3, zipCode='')
    self._make_biobank_order(participantId=participant3.participantId,
                             biobankOrderId=participant3.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'https://www.pmi-ops.org',
                                                                 value=u'12345680')])

    participant4 = self._make_participant()
    self._make_summary(participant4)
    self._make_biobank_order(participantId=participant4.participantId,
                             biobankOrderId=participant4.participantId,
                             identifiers=[BiobankOrderIdentifier(system=u'c', value=u'e')])

    samples_file = test_data.open_genomic_set_file('Genomic-Test-Set-test-3.csv')

    input_filename = 'Genomic-Test-Set-v1%s.csv' % self\
      ._naive_utc_to_naive_central(clock.CLOCK.now())\
      .strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(input_filename, samples_file)

    genomic_pipeline.process_genomic_water_line()

    # verify result file
    bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
    path = self._find_latest_genomic_set_csv(bucket_name, 'Validation-Result')
    csv_file = cloudstorage_api.open(path)
    csv_reader = csv.DictReader(csv_file, delimiter=',')

    class ResultCsvColumns(object):
      """Names of CSV columns that we read from the genomic set upload."""
      GENOMIC_SET_NAME = 'genomic_set_name'
      GENOMIC_SET_CRITERIA = 'genomic_set_criteria'
      PID = 'pid'
      BIOBANK_ORDER_ID = 'biobank_order_id'
      NY_FLAG = 'ny_flag'
      SEX_AT_BIRTH = 'sex_at_birth'
      GENOME_TYPE = 'genome_type'
      STATUS = 'status'
      INVALID_REASON = 'invalid_reason'

      ALL = (GENOMIC_SET_NAME, GENOMIC_SET_CRITERIA, PID, BIOBANK_ORDER_ID, NY_FLAG, SEX_AT_BIRTH,
             GENOME_TYPE, STATUS, INVALID_REASON)

    missing_cols = set(ResultCsvColumns.ALL) - set(csv_reader.fieldnames)
    self.assertEqual(len(missing_cols), 0)
    rows = list(csv_reader)
    self.assertEqual(len(rows), 4)
    self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_NAME], 'name_xxx')
    self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'criteria_xxx')
    self.assertEqual(rows[0][ResultCsvColumns.STATUS], 'invalid')
    self.assertEqual(rows[0][ResultCsvColumns.INVALID_REASON], 'INVALID_AGE, INVALID_NY_ZIPCODE')
    self.assertEqual(rows[0][ResultCsvColumns.PID], '1')
    self.assertEqual(rows[0][ResultCsvColumns.BIOBANK_ORDER_ID], '1')
    self.assertEqual(rows[0][ResultCsvColumns.NY_FLAG], 'Y')
    self.assertEqual(rows[0][ResultCsvColumns.GENOME_TYPE], 'aou_wgs')
    self.assertEqual(rows[0][ResultCsvColumns.SEX_AT_BIRTH], 'M')

    self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_NAME], 'name_xxx')
    self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'criteria_xxx')
    self.assertEqual(rows[1][ResultCsvColumns.STATUS], 'invalid')
    self.assertEqual(rows[1][ResultCsvColumns.INVALID_REASON], 'INVALID_CONSENT')
    self.assertEqual(rows[1][ResultCsvColumns.PID], '2')
    self.assertEqual(rows[1][ResultCsvColumns.BIOBANK_ORDER_ID], '2')
    self.assertEqual(rows[1][ResultCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[1][ResultCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[1][ResultCsvColumns.SEX_AT_BIRTH], 'F')

    self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_NAME], 'name_xxx')
    self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'criteria_xxx')
    self.assertEqual(rows[2][ResultCsvColumns.STATUS], 'invalid')
    self.assertEqual(rows[2][ResultCsvColumns.INVALID_REASON], 'INVALID_NY_ZIPCODE')
    self.assertEqual(rows[2][ResultCsvColumns.PID], '3')
    self.assertEqual(rows[2][ResultCsvColumns.BIOBANK_ORDER_ID], '3')
    self.assertEqual(rows[2][ResultCsvColumns.NY_FLAG], 'N')
    self.assertEqual(rows[2][ResultCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[2][ResultCsvColumns.SEX_AT_BIRTH], 'M')

  def _create_fake_genomic_set(self, genomic_set_name, genomic_set_criteria, genomic_set_filename):
    now = clock.CLOCK.now()
    genomic_set = GenomicSet()
    genomic_set.genomicSetName = genomic_set_name
    genomic_set.genomicSetCriteria = genomic_set_criteria
    genomic_set.genomicSetFile = genomic_set_filename
    genomic_set.genomicSetFileTime = now
    genomic_set.genomicSetStatus = GenomicSetStatus.INVALID

    set_dao = GenomicSetDao()
    genomic_set.genomicSetVersion = set_dao.get_new_version_number(genomic_set.genomicSetName)

    set_dao.insert(genomic_set)

    return genomic_set

  def _create_fake_genomic_member(self, genomic_set_id, participant_id, biobank_order_id,
                                  validation_status=GenomicSetMemberStatus.VALID,
                                  validation_flags=None,
                                  sex_at_birth='F', genome_type='aou_array', ny_flag='Y'):
    genomic_set_member = GenomicSetMember()
    genomic_set_member.genomicSetId = genomic_set_id
    genomic_set_member.validationStatus = validation_status
    genomic_set_member.validationFlags = validation_flags
    genomic_set_member.participantId = participant_id
    genomic_set_member.sexAtBirth = sex_at_birth
    genomic_set_member.genomeType = genome_type
    genomic_set_member.nyFlag = 1 if ny_flag == 'Y' else 0
    genomic_set_member.biobankOrderId = biobank_order_id

    member_dao = GenomicSetMemberDao()
    member_dao.insert(genomic_set_member)

  def _naive_utc_to_naive_central(self, naive_utc_date):
    utc_date = pytz.utc.localize(naive_utc_date)
    central_date = utc_date.astimezone(pytz.timezone('US/Central'))
    return central_date.replace(tzinfo=None)

  def _find_latest_genomic_set_csv(self, cloud_bucket_name, keyword=None):
    bucket_stat_list = cloudstorage_api.listbucket('/' + cloud_bucket_name)
    if not bucket_stat_list:
      raise RuntimeError('No files in cloud bucket %r.' % cloud_bucket_name)
    bucket_stat_list = [s for s in bucket_stat_list if s.filename.lower().endswith('.csv')]
    if not bucket_stat_list:
      raise RuntimeError(
        'No CSVs in cloud bucket %r (all files: %s).' % (cloud_bucket_name, bucket_stat_list))
    if keyword:
      buckt_stat_keyword_list = []
      for item in bucket_stat_list:
        if keyword in item.filename:
          buckt_stat_keyword_list.append(item)
      if buckt_stat_keyword_list:
        buckt_stat_keyword_list.sort(key=lambda s: s.st_ctime)
        return buckt_stat_keyword_list[-1].filename
      else:
        raise RuntimeError(
          'No CSVs in cloud bucket %r with keyword %s (all files: %s).' % (cloud_bucket_name,
                                                                           keyword,
                                                                           bucket_stat_list))
    bucket_stat_list.sort(key=lambda s: s.st_ctime)
    return bucket_stat_list[-1].filename
