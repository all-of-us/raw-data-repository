import StringIO
import csv
import datetime
import random
import time

import clock
import config
import pytz
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
from model.genomics import GenomicSet

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

  def _naive_utc_to_naive_central(self, naive_utc_date):
    utc_date = pytz.utc.localize(naive_utc_date)
    central_date = utc_date.astimezone(pytz.timezone('US/Central'))
    return central_date.replace(tzinfo=None)
