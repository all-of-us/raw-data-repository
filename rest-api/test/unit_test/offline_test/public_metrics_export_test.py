import datetime

from offline.public_metrics_export import PublicMetricsExport
from clock import FakeClock
from code_constants import CONSENT_PERMISSION_YES_CODE, CONSENT_PERMISSION_NO_CODE
from code_constants import EHR_CONSENT_QUESTION_CODE, RACE_WHITE_CODE
from code_constants import RACE_NONE_OF_THESE_CODE, PMI_PREFER_NOT_TO_ANSWER_CODE
from field_mappings import FIELD_TO_QUESTIONNAIRE_MODULE_CODE
from model.biobank_stored_sample import BiobankStoredSample
from model.code import CodeType
from model.hpo import HPO
from model.participant import Participant
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.hpo_dao import HPODao
from dao.metric_set_dao import AggregateMetricsDao, MetricSetDao
from dao.participant_dao import ParticipantDao, make_primary_provider_link_for_name
from offline.metrics_config import ANSWER_FIELD_TO_QUESTION_CODE
from participant_enums import WithdrawalStatus
from test_data import load_biobank_order_json, load_measurement_json
from unit_test_util import FlaskTestBase, CloudStorageSqlTestBase, SqlTestBase, TestBase
from unit_test_util import PITT_HPO_ID

TIME = datetime.datetime(2016, 1, 1)
TIME2 = datetime.datetime(2016, 2, 2)


class PublicMetricsExportTest(CloudStorageSqlTestBase, FlaskTestBase):
  """Test the public metrics recalculation."""

  def setUp(self):
    super(PublicMetricsExportTest, self).setUp()
    FlaskTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    self.maxDiff = None

  def tearDown(self):
    super(PublicMetricsExportTest, self).tearDown()
    FlaskTestBase.doTearDown(self)

  def _create_data(self):
    HPODao().insert(HPO(hpoId=PITT_HPO_ID + 1, name='AZ_TUCSON'))
    HPODao().insert(HPO(hpoId=PITT_HPO_ID + 2, name='TEST'))
    SqlTestBase.setup_codes(
        ANSWER_FIELD_TO_QUESTION_CODE.values() + [EHR_CONSENT_QUESTION_CODE],
        code_type=CodeType.QUESTION)
    SqlTestBase.setup_codes(
        FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values(), code_type=CodeType.MODULE)
    # Import codes for white and female, but not male or black.
    SqlTestBase.setup_codes(
        [
            RACE_WHITE_CODE, CONSENT_PERMISSION_YES_CODE,
            RACE_NONE_OF_THESE_CODE, PMI_PREFER_NOT_TO_ANSWER_CODE,
            CONSENT_PERMISSION_NO_CODE, 'female', 'PIIState_VA'
        ],
        code_type=CodeType.ANSWER)
    participant_dao = ParticipantDao()

    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    questionnaire_id_2 = self.create_questionnaire('questionnaire4.json')
    questionnaire_id_3 = self.create_questionnaire(
        'all_consents_questionnaire.json')

    with FakeClock(TIME):
      participant = self._participant_with_defaults(
          participantId=1,
          version=2,
          biobankId=2,
          providerLink=make_primary_provider_link_for_name('PITT'))
      participant_dao.insert(participant)
      self.send_consent('P1', email='bob@gmail.com')

      # Participant 2 starts out unpaired; later gets paired automatically when their physical
      # measurements come in.
      participant2 = Participant(participantId=2, biobankId=3)
      participant_dao.insert(participant2)
      self.send_consent('P2', email='bob@fexample.com')

      # Test HPO affiliation; this test participant is ignored.
      participant3 = Participant(
          participantId=3,
          biobankId=4,
          providerLink=make_primary_provider_link_for_name('TEST'))
      participant_dao.insert(participant3)
      self.send_consent('P3', email='fred@gmail.com')

      # example.com e-mail; this test participant is ignored, too.
      participant4 = Participant(
          participantId=4,
          biobankId=5,
          providerLink=make_primary_provider_link_for_name('PITT'))
      participant_dao.insert(participant4)
      self.send_consent('P4', email='bob@example.com')

      participant5 = Participant(
          participantId=5,
          biobankId=6,
          providerLink=make_primary_provider_link_for_name('PITT'))
      participant_dao.insert(participant5)
      self.send_consent('P5', email='ch@gmail.com')

      # A withdrawn participant should be excluded from metrics.
      participant6 = Participant(
          participantId=6,
          biobankId=7,
          providerLink=make_primary_provider_link_for_name('PITT')
      )
      participant_dao.insert(participant6)
      self.send_consent('P6', email='cuphead@gmail.com')
      participant6.withdrawalStatus=WithdrawalStatus.NO_USE
      participant_dao.update(participant6)

      self.send_post('Participant/P2/PhysicalMeasurements',
                     load_measurement_json(2))
      self.send_post('Participant/P2/BiobankOrder', load_biobank_order_json(2))
      self.submit_questionnaire_response('P1', questionnaire_id,
                                         RACE_WHITE_CODE, 'female', None,
                                         datetime.date(1980, 1, 2))
      self.submit_questionnaire_response(
          'P2', questionnaire_id, PMI_PREFER_NOT_TO_ANSWER_CODE, 'male', None,
          datetime.date(1920, 1, 3))
      self.submit_questionnaire_response('P2', questionnaire_id_2, None, None,
                                         'PIIState_VA', None)
      self.submit_questionnaire_response('P5', questionnaire_id, None, None,
                                         None, datetime.date(1970, 1, 2))
      self.submit_consent_questionnaire_response('P1', questionnaire_id_3,
                                                 CONSENT_PERMISSION_NO_CODE)
      self.submit_consent_questionnaire_response('P2', questionnaire_id_3,
                                                 CONSENT_PERMISSION_YES_CODE)
      sample_dao = BiobankStoredSampleDao()
      sample_dao.insert(
          BiobankStoredSample(
              biobankStoredSampleId='abc',
              biobankId=2,
              test='test',
              confirmed=TIME))
      sample_dao.insert(
          BiobankStoredSample(
              biobankStoredSampleId='def',
              biobankId=3,
              test='1SAL',
              confirmed=TIME))
      # Required to update the HPO linkage (and test filtering for P3).
      sample_dao.insert(
          BiobankStoredSample(
              biobankStoredSampleId='xyz',
              biobankId=4,
              test='1SAL',
              confirmed=TIME))


  def assert_total_count_per_key(self, want_total_count):
    agg_by_key = {}
    for agg in AggregateMetricsDao().get_all():
      if agg.metricsKey not in agg_by_key:
        agg_by_key[agg.metricsKey] = []
      agg_by_key[agg.metricsKey].append(agg)

    self.assertNotEquals(0, len(agg_by_key), 'no metrics were persisted')

    for (k, aggs) in agg_by_key.iteritems():
      count = sum([agg.count for agg in aggs])
      self.assertEquals(want_total_count, count,
                        ('metric {} must contain aggregates over exactly '
                         'the set of {} qualified participants, got {}').format(
                             k, want_total_count, count))


  def test_metrics_export(self):
    self._create_data()

    with FakeClock(TIME):
      PublicMetricsExport.export('123')
      self.assert_total_count_per_key(3) # 3 qualified participants


  def test_metrics_update(self):
    self._create_data()

    with FakeClock(TIME):
      PublicMetricsExport.export('123')
    aggs1 = [a.asdict() for a in AggregateMetricsDao().get_all()]

    with FakeClock(TIME2):
      PublicMetricsExport.export('123')
    aggs2 = [a.asdict() for a in AggregateMetricsDao().get_all()]

    self.assertEquals(TIME2, MetricSetDao().get('123').lastModified)
    self.assertEquals(aggs1, aggs2)


  def test_metrics_redaction(self):
    self._create_data()

    with FakeClock(TIME):
      PublicMetricsExport.export('123')

      # Withdraw particpant.
      pdao = ParticipantDao()
      p1 = pdao.get(1)
      p1.withdrawalStatus = WithdrawalStatus.NO_USE
      pdao.update(p1)

      PublicMetricsExport.export('123')
      self.assert_total_count_per_key(2) # now, 2 qualified participants
