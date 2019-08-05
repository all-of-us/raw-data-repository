import datetime
import json

from test_data import load_measurement_json
from unit_test_util import SqlTestBase

from rdr_service import clock
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.dao.bigquery_sync_dao import BQParticipantSummaryGenerator
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.site import Site
from rdr_service.participant_enums import WithdrawalStatus, SuspensionStatus


class BigQuerySyncDaoTest(SqlTestBase):

  TIME_1 = datetime.datetime(2018, 9, 20, 5, 49, 11)
  TIME_2 = datetime.datetime(2018, 9, 24, 14, 21, 01)

  site = None
  hpo = None
  participant = None
  summary = None
  pm_json = None
  pm = None
  bio_order = None

  def setUp(self):

    super(BigQuerySyncDaoTest, self).setUp(use_mysql=True, with_consent_codes=True)
    self.dao = ParticipantDao()

    with self.dao.session() as session:
      self.site = session.query(Site).filter(Site.googleGroup == 'hpo-site-monroeville').first()
      self.hpo = session.query(HPO).filter(HPO.name == 'PITT').first()

    with clock.FakeClock(self.TIME_1):
      self.participant = Participant(participantId=123, biobankId=555)
      self.participant.hpoId = self.hpo.hpoId
      self.participant.siteId = self.site.siteId
      self.dao.insert(self.participant)

      ps = ParticipantSummary(participantId=123, biobankId=555, firstName='john', lastName='doe',
                              withdrawalStatus=WithdrawalStatus.NOT_WITHDRAWN, suspensionStatus=SuspensionStatus.NOT_SUSPENDED)
      ps.hpoId = self.hpo.hpoId
      ps.siteId = self.site.siteId
      self.summary = ParticipantSummaryDao().insert(ps)

    self.pm_json = json.dumps(load_measurement_json(self.participant.participantId, self.TIME_1.isoformat()))
    self.pm = PhysicalMeasurementsDao().insert(self._make_physical_measurements())

    with clock.FakeClock(self.TIME_2):
      self.dao = BiobankOrderDao()
      self.bio_order = BiobankOrderDao().insert(self._make_biobank_order(participantId=self.participant.participantId))


  def _make_physical_measurements(self, **kwargs):
    """Makes a new PhysicalMeasurements (same values every time) with valid/complete defaults.
    Kwargs pass through to PM constructor, overriding defaults.
    """
    for k, default_value in (
        ('physicalMeasurementsId', 1),
        ('participantId', self.participant.participantId),
        ('resource', self.pm_json),
        ('createdSiteId', self.site.siteId),
        ('finalizedSiteId', self.site.siteId)):
      if k not in kwargs:
        kwargs[k] = default_value
    return PhysicalMeasurements(**kwargs)

  def _make_biobank_order(self, **kwargs):
    """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
    for k, default_value in (
        ('biobankOrderId', '1'),
        ('created', clock.CLOCK.now()),
        ('participantId', self.participant.participantId),
        ('sourceSiteId', 1),
        ('sourceUsername', 'fred@pmi-ops.org'),
        ('collectedSiteId', 1),
        ('collectedUsername', 'joe@pmi-ops.org'),
        ('processedSiteId', 1),
        ('processedUsername', 'sue@pmi-ops.org'),
        ('finalizedSiteId', 2),
        ('finalizedUsername', 'bob@pmi-ops.org'),
        ('identifiers', [BiobankOrderIdentifier(system='a', value='c')]),
        ('samples', [BiobankOrderedSample(
            biobankOrderId='1',
            test=BIOBANK_TESTS[0],
            description=u'description',
            finalized=self.TIME_1,
            processingRequired=True)])):
      if k not in kwargs:
        kwargs[k] = default_value
    return BiobankOrder(**kwargs)

  def test_participant_summary_gen(self):

    gen = BQParticipantSummaryGenerator()
    ps_json = gen.make_participant_summary(self.participant.participantId)

    self.assertIsNotNone(ps_json)
    self.assertEqual(ps_json.sign_up_time, self.TIME_1)
    self.assertEqual(ps_json['pm'][0]['pm_finalized_site'], 'hpo-site-monroeville')
    self.assertEqual(ps_json.suspension_status, 'NOT_SUSPENDED')
    self.assertEqual(ps_json.withdrawal_status, 'NOT_WITHDRAWN')
    self.assertEqual(ps_json['pm'][0]['pm_status'], 'UNSET')
