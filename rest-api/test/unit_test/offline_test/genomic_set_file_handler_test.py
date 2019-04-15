import datetime

import clock
import config
import pytz
import csv
from cloudstorage import cloudstorage_api  # stubbed by testbed
from code_constants import BIOBANK_TESTS
from model.participant import Participant
from offline import genomic_set_file_handler
from test import test_data
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase
from dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.biobank_dv_order import BiobankDVOrder
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus, GenomicValidationStatus

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = 'rdr_fake_bucket'


class GenomicSetFileHandlerTest(CloudStorageSqlTestBase, NdbTestBase):
  def setUp(self):
    super(GenomicSetFileHandlerTest, self).setUp(use_mysql=True)
    NdbTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    # Everything is stored as a list, so override bucket name as a 1-element list.
    config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()

  def _write_cloud_csv(self, file_name, contents_str):
    with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
      cloud_file.write(contents_str.encode('utf-8'))

  def _make_biobank_order(self, **kwargs):
    """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
    participant_id = kwargs['participantId']
    modified = datetime.datetime(2019, 03, 25, 15, 59, 30)

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
          participantId=participant_id, modified=modified, version=1)])):
      if k not in kwargs:
        kwargs[k] = default_value
    return BiobankOrder(**kwargs)

  def test_read_from_csv_file(self):
    participant = self.participant_dao.insert(Participant(participantId=123, biobankId=123))
    self.summary_dao.insert(self.participant_summary(participant))
    bo = self._make_biobank_order(participantId=participant.participantId, biobankOrderId='123',
                                  identifiers=[BiobankOrderIdentifier(system=u'a', value=u'c')])
    BiobankOrderDao().insert(bo)

    participant2 = self.participant_dao.insert(Participant(participantId=124, biobankId=124))
    self.summary_dao.insert(self.participant_summary(participant2))
    bo2 = self._make_biobank_order(participantId=participant2.participantId, biobankOrderId='124',
                                   identifiers=[BiobankOrderIdentifier(system=u'b', value=u'd')])
    BiobankOrderDao().insert(bo2)

    participant3 = self.participant_dao.insert(Participant(participantId=125, biobankId=125))
    self.summary_dao.insert(self.participant_summary(participant3))
    bo3 = self._make_biobank_order(participantId=participant3.participantId, biobankOrderId='125',
                                   identifiers=[BiobankOrderIdentifier(system=u'c', value=u'e')])
    BiobankOrderDao().insert(bo3)

    samples_file = test_data.open_genomic_set_file()

    input_filename = 'cloud%s.csv' % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
        genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

    self._write_cloud_csv(input_filename, samples_file)
    genomic_set_file_handler.read_genomic_set_from_bucket()
    set_dao = GenomicSetDao()
    obj = set_dao.get_all()[0]

    self.assertEqual(obj.genomicSetName, 'name_xxx')
    self.assertEqual(obj.genomicSetCriteria, 'criteria_xxx')
    self.assertEqual(obj.genomicSetVersion, 1)

    member_dao = GenomicSetMemberDao()
    items = member_dao.get_all()
    for item in items:
      self.assertIn(item.participantId, [123, 124, 125])
      self.assertIn(item.biobankOrderId, ['123', '124', '125'])
      self.assertEqual(item.genomicSetId, 1)
      self.assertIn(item.genomeType, ['aou_wgs', 'aou_array'])
      self.assertIn(item.nyFlag, [0, 1])
      self.assertIn(item.sexAtBirth, ['F', 'M'])

  def test_create_genomic_set_result_file(self):
    participant = self.participant_dao.insert(Participant(participantId=123, biobankId=123))
    self.summary_dao.insert(self.participant_summary(participant))
    bo = self._make_biobank_order(participantId=participant.participantId, biobankOrderId='123',
                                  identifiers=[BiobankOrderIdentifier(system=u'a', value=u'c')])
    BiobankOrderDao().insert(bo)

    participant2 = self.participant_dao.insert(Participant(participantId=124, biobankId=124))
    self.summary_dao.insert(self.participant_summary(participant2))
    bo2 = self._make_biobank_order(participantId=participant2.participantId, biobankOrderId='124',
                                   identifiers=[BiobankOrderIdentifier(system=u'b', value=u'd')])
    BiobankOrderDao().insert(bo2)

    participant3 = self.participant_dao.insert(Participant(participantId=125, biobankId=125))
    self.summary_dao.insert(self.participant_summary(participant3))
    bo3 = self._make_biobank_order(participantId=participant3.participantId, biobankOrderId='125',
                                   identifiers=[BiobankOrderIdentifier(system=u'c', value=u'e')])
    BiobankOrderDao().insert(bo3)

    genomic_set = self._create_fake_genomic_set('fake_genomic_set_name',
                                                'fake_genomic_set_criteria',
                                                'Genomic-Test-Set-v12019-04-05-00-30-10.CSV')
    self._create_fake_genomic_member(genomic_set.id, participant.participantId, bo.biobankOrderId,
                                     validation_status=GenomicValidationStatus.VALID,
                                     sex_at_birth='F', genome_type='aou_array', ny_flag='Y')

    self._create_fake_genomic_member(genomic_set.id, participant2.participantId, bo2.biobankOrderId,
                                     validation_status=GenomicValidationStatus.INVALID_AGE,
                                     sex_at_birth='M', genome_type='aou_array', ny_flag='N')

    self._create_fake_genomic_member(genomic_set.id, participant3.participantId, bo3.biobankOrderId,
                                     validation_status=GenomicValidationStatus.INVALID_CONSENT,
                                     sex_at_birth='F', genome_type='aou_wgs', ny_flag='Y')

    genomic_set_file_handler.create_genomic_set_status_result_file(genomic_set.id)

    expected_result_filename = 'Genomic-Test-Set-v12019-04-05-00-30-10-Validation-Result.CSV'
    bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
    path = '/' + bucket_name + '/' + expected_result_filename
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
    self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_NAME], 'fake_genomic_set_name')
    self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'fake_genomic_set_criteria')
    self.assertEqual(rows[0][ResultCsvColumns.STATUS], 'valid')
    self.assertEqual(rows[0][ResultCsvColumns.INVALID_REASON], '')
    self.assertEqual(rows[0][ResultCsvColumns.PID], '123')
    self.assertEqual(rows[0][ResultCsvColumns.BIOBANK_ORDER_ID], '123')
    self.assertEqual(rows[0][ResultCsvColumns.NY_FLAG], '1')
    self.assertEqual(rows[0][ResultCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[0][ResultCsvColumns.SEX_AT_BIRTH], 'F')

    self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_NAME], 'fake_genomic_set_name')
    self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'fake_genomic_set_criteria')
    self.assertEqual(rows[1][ResultCsvColumns.STATUS], 'invalid')
    self.assertEqual(rows[1][ResultCsvColumns.INVALID_REASON], 'INVALID_AGE')
    self.assertEqual(rows[1][ResultCsvColumns.PID], '124')
    self.assertEqual(rows[1][ResultCsvColumns.BIOBANK_ORDER_ID], '124')
    self.assertEqual(rows[1][ResultCsvColumns.NY_FLAG], '0')
    self.assertEqual(rows[1][ResultCsvColumns.GENOME_TYPE], 'aou_array')
    self.assertEqual(rows[1][ResultCsvColumns.SEX_AT_BIRTH], 'M')

    self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_NAME], 'fake_genomic_set_name')
    self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_CRITERIA], 'fake_genomic_set_criteria')
    self.assertEqual(rows[2][ResultCsvColumns.STATUS], 'invalid')
    self.assertEqual(rows[2][ResultCsvColumns.INVALID_REASON], 'INVALID_CONSENT')
    self.assertEqual(rows[2][ResultCsvColumns.PID], '125')
    self.assertEqual(rows[2][ResultCsvColumns.BIOBANK_ORDER_ID], '125')
    self.assertEqual(rows[2][ResultCsvColumns.NY_FLAG], '1')
    self.assertEqual(rows[2][ResultCsvColumns.GENOME_TYPE], 'aou_wgs')
    self.assertEqual(rows[2][ResultCsvColumns.SEX_AT_BIRTH], 'F')

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
    genomic_set.created = now
    genomic_set.modified = now

    set_dao.insert(genomic_set)

    return genomic_set

  def _create_fake_genomic_member(self, genomic_set_id, participant_id, biobank_order_id,
                                  validation_status=GenomicValidationStatus.VALID,
                                  sex_at_birth='F', genome_type='aou_array', ny_flag='Y'):
    now = clock.CLOCK.now()
    genomic_set_member = GenomicSetMember()
    genomic_set_member.genomicSetId = genomic_set_id
    genomic_set_member.created = now
    genomic_set_member.modified = now
    genomic_set_member.validationStatus = validation_status
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
