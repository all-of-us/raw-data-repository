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
from dao.participant_dao import ParticipantDao, make_primary_provider_link_for_name
from offline.metrics_config import ANSWER_FIELD_TO_QUESTION_CODE
from test_data import load_biobank_order_json, load_measurement_json
from unit_test_util import FlaskTestBase, CloudStorageSqlTestBase, SqlTestBase, TestBase
from unit_test_util import PITT_HPO_ID

TIME = datetime.datetime(2016, 1, 1)


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

  def test_metric_export(self):
    self._create_data()
    want = {
        'physicalMeasurements': [{
            'count': 2,
            'value': 'UNSET'
        }, {
            'count': 1,
            'value': 'COMPLETED'
        }],
        'gender': [{
            'count': 1,
            'value': u'UNSET'
        }, {
            'count': 1,
            'value': u'female'
        }, {
            'count': 1,
            'value': u'male'
        }],
        'questionnaireOnOverallHealth': [{
            'count': 2,
            'value': 'UNSET'
        }, {
            'count': 1,
            'value': 'SUBMITTED'
        }],
        'biospecimenSamples': [{
            'count': 1,
            'value': u'COLLECTED'
        }, {
            'count': 2,
            'value': u'UNSET'
        }],
        'questionnaireOnPersonalHabits': [{
            'count': 2,
            'value': 'UNSET'
        }, {
            'count': 1,
            'value': 'SUBMITTED'
        }],
        'state': [{
            'count': 2,
            'value': u'UNSET'
        }, {
            'count': 1,
            'value': u'VA'
        }],
        'race': [{
            'count': 1,
            'value': 'UNSET'
        }, {
            'count': 1,
            'value': 'WHITE'
        }, {
            'count': 1,
            'value': 'PREFER_NOT_TO_SAY'
        }],
        'enrollmentStatus': [{
            'count': 3,
            'value': 'INTERESTED'
        }],
        'questionnaireOnSociodemographics': [{
            'count': 3,
            'value': 'SUBMITTED'
        }],
        'ageRange': [{
            'count': 1,
            'value': u'36-45'
        }, {
            'count': 1,
            'value': u'46-55'
        }, {
            'count': 1,
            'value': u'86+'
        }]
    }
    with FakeClock(TIME):
      self.assertEquals(want, PublicMetricsExport.export())
