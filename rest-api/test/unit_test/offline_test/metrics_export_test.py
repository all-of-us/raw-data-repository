import datetime

from clock import FakeClock
from code_constants import PPI_SYSTEM, METRIC_FIELD_TO_QUESTION_CODE
from code_constants import FIELD_TO_QUESTIONNAIRE_MODULE_CODE, GENDER_IDENTITY_QUESTION_CODE
from code_constants import RACE_QUESTION_CODE, ETHNICITY_QUESTION_CODE
from concepts import Concept
from model.biobank_stored_sample import BiobankStoredSample
from model.code import CodeType
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from model.participant import Participant
from offline.metrics_export import MetricsExport, HPO_IDS_CSV, PARTICIPANTS_CSV, ANSWERS_CSV
from offline_test.gcs_utils import assertCsvContents
from participant_enums import UNSET_HPO_ID
from test_data import primary_provider_link, load_biobank_order_json, load_measurement_json
from unit_test_util import FlaskTestBase, CloudStorageSqlTestBase, SqlTestBase, PITT_HPO_ID, run_deferred_tasks
from unit_test_util import make_questionnaire_response_json


BUCKET_NAME = 'pmi-drc-biobank-test.appspot.com'
TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
PARTICIPANT_FIELDS = ['date_of_birth', 'first_order_date', 'first_samples_arrived_date',
                      'first_physical_measurements_date',
                      'questionnaire_on_overall_health_time',
                      'questionnaire_on_personal_habits_time',
                      'questionnaire_on_sociodemographics_time']
HPO_ID_FIELDS = ['participant_id', 'hpo_id', 'last_modified']
ANSWER_FIELDS = ['participant_id', 'start_time', 'end_time', 'question_code', 'answer_code']


class MetricsExportTest(CloudStorageSqlTestBase, FlaskTestBase):
  def setUp(self):
    super(MetricsExportTest, self).setUp()
    FlaskTestBase.doSetUp(self)
    self.taskqueue.FlushQueue('default')

  def tearDown(self):
    super(MetricsExportTest, self).tearDown()
    FlaskTestBase.doTearDown(self)

  def submit_questionnaire_response(self, participant_id, questionnaire_id,
                                    race_code, gender_code, ethnicity_code,
                                    date_of_birth):
    code_answers = []
    date_answers = []
    if race_code:
      code_answers.append(('race', Concept(PPI_SYSTEM, race_code)))
    if gender_code:
      code_answers.append(('genderIdentity', Concept(PPI_SYSTEM, gender_code)))
    if ethnicity_code:
      code_answers.append(('ethnicity', Concept(PPI_SYSTEM, ethnicity_code)))
    if date_of_birth:
      date_answers.append(('dateOfBirth', date_of_birth))
    qr = make_questionnaire_response_json(participant_id,
                                          questionnaire_id,
                                          code_answers = code_answers,
                                          date_answers = date_answers)
    self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def _create_data(self):
    SqlTestBase.setup_codes(METRIC_FIELD_TO_QUESTION_CODE.values(),
                            code_type=CodeType.QUESTION)
    SqlTestBase.setup_codes(FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values(),
                            code_type=CodeType.MODULE)
    participant_dao = ParticipantDao()

    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    questionnaire_id_2 = self.create_questionnaire('questionnaire4.json')
    with FakeClock(TIME):
      participant = Participant(participantId=1, biobankId=2)
      participant_dao.insert(participant)

    with FakeClock(TIME):
      participant2 = Participant(participantId=2, biobankId=3)
      participant_dao.insert(participant2)

    with FakeClock(TIME_2):
      participant = Participant(participantId=1, version=1, biobankId=2,
                                providerLink=primary_provider_link('PITT'))
      participant_dao.update(participant)
      self.submit_questionnaire_response('P1', questionnaire_id, 'white', 'male',
                                         'hispanic', datetime.date(1978, 10, 9))
      self.submit_questionnaire_response('P2', questionnaire_id, None, None, None, None)

    with FakeClock(TIME_3):
      self.send_post('Participant/P2/PhysicalMeasurements', load_measurement_json(2))
      self.send_post('Participant/P2/BiobankOrder', load_biobank_order_json(2))
      self.submit_questionnaire_response('P1', questionnaire_id, "black", "female",
                                         "hispanic", datetime.date(1978, 10, 10))
      self.submit_questionnaire_response('P2', questionnaire_id_2, None, None, None, None)
      sample_dao = BiobankStoredSampleDao()
      sample_dao.insert(BiobankStoredSample(
        biobankStoredSampleId='abc',
        biobankId=2,
        test='test',
        confirmed=TIME_2))

  def disabled_test_metric_export(self):
    # TODO(DA-228) Fix and re-enable. The _create_data call fails locally due to 'foo' in
    # BASELINE_PPI_QUESTIONNAIRE_FIELDS (the other value is 'questionnaireOnSociodemographics'), but
    # only when running other unit tests as well as this one.
    self._create_data()

    MetricsExport.start_export_tasks(BUCKET_NAME, TIME_3, 2)
    run_deferred_tasks(self)

    t1 = TIME.strftime(TIME_FORMAT)
    t2 = TIME_2.strftime(TIME_FORMAT)
    t3 = TIME_3.strftime(TIME_FORMAT)
    prefix = TIME_3.isoformat() + "/"

    # Two shards are written for each file, one with the first participant and
    # one with the second.
    assertCsvContents(self, BUCKET_NAME, prefix + HPO_IDS_CSV % 0,
                      [HPO_ID_FIELDS,
                       ['2', str(UNSET_HPO_ID), t1]])
    assertCsvContents(self, BUCKET_NAME, prefix + HPO_IDS_CSV % 1,
                      [HPO_ID_FIELDS,
                       ['1', str(UNSET_HPO_ID), t1],
                       ['1', str(PITT_HPO_ID), t2]])
    assertCsvContents(self, BUCKET_NAME, prefix + PARTICIPANTS_CSV % 0,
                      [PARTICIPANT_FIELDS,
                       ['', '2016-01-04T09:40:21Z', '', t3, t3, t3, t2]])
    assertCsvContents(self, BUCKET_NAME, prefix + PARTICIPANTS_CSV % 1,
                      [PARTICIPANT_FIELDS,
                       ['1978-10-10', '', t2, '', '', '', t2]])
    assertCsvContents(self, BUCKET_NAME, prefix + ANSWERS_CSV % 0,
                      [ANSWER_FIELDS])
    assertCsvContents(self, BUCKET_NAME, prefix + ANSWERS_CSV % 1,
                      [ANSWER_FIELDS,
                       ['1', t2, t3, GENDER_IDENTITY_QUESTION_CODE, 'male'],
                       ['1', t2, t3, RACE_QUESTION_CODE, 'white'],
                       ['1', t2, t3, ETHNICITY_QUESTION_CODE, 'hispanic'],
                       ['1', t3, '', GENDER_IDENTITY_QUESTION_CODE, 'female'],
                       ['1', t3, '', RACE_QUESTION_CODE, 'black'],
                       ['1', t3, '', ETHNICITY_QUESTION_CODE, 'hispanic']])

