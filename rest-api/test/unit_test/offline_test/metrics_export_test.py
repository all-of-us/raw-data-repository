import datetime
import json
import offline.metrics_export

from clock import FakeClock
from code_constants import PPI_SYSTEM
from code_constants import FIELD_TO_QUESTIONNAIRE_MODULE_CODE, GENDER_IDENTITY_QUESTION_CODE
from code_constants import RACE_QUESTION_CODE, ETHNICITY_QUESTION_CODE, STATE_QUESTION_CODE
from concepts import Concept
from mapreduce import test_support
from model.biobank_stored_sample import BiobankStoredSample
from model.code import CodeType
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.metrics_dao import MetricsVersionDao, SERVING_METRICS_DATA_VERSION
from dao.participant_dao import ParticipantDao
from model.metrics import MetricsVersion
from model.participant import Participant
from offline.metrics_config import ANSWER_FIELD_TO_QUESTION_CODE
from offline.metrics_config import get_participant_fields, HPO_ID_FIELDS, ANSWER_FIELDS
from offline.metrics_export import MetricsExport, HPO_IDS_CSV, PARTICIPANTS_CSV, ANSWERS_CSV
from offline_test.gcs_utils import assertCsvContents
from test_data import primary_provider_link, load_biobank_order_json, load_measurement_json
from unit_test_util import FlaskTestBase, CloudStorageSqlTestBase, SqlTestBase, PITT_HPO_ID, 
from unit_test_util import make_questionnaire_response_json, pretty, run_deferred_tasks

BUCKET_NAME = 'pmi-drc-biobank-test.appspot.com'
TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

class MetricsExportTest(CloudStorageSqlTestBase, FlaskTestBase):
  """Tests exporting of metrics AND the output of the metrics pipeline that runs when the
   export completes."""
  def setUp(self):
    super(MetricsExportTest, self).setUp()
    FlaskTestBase.doSetUp(self)
    offline.metrics_export.QUEUE_NAME = 'default'
    self.taskqueue.FlushQueue('default')
    self.maxDiff = None

  def tearDown(self):
    super(MetricsExportTest, self).tearDown()
    FlaskTestBase.doTearDown(self)

  def submit_questionnaire_response(self, participant_id, questionnaire_id,
                                    race_code, gender_code, ethnicity_code,
                                    state, date_of_birth):
    code_answers = []
    date_answers = []
    string_answers = []
    if race_code:
      code_answers.append(('race', Concept(PPI_SYSTEM, race_code)))
    if gender_code:
      code_answers.append(('genderIdentity', Concept(PPI_SYSTEM, gender_code)))
    if ethnicity_code:
      code_answers.append(('ethnicity', Concept(PPI_SYSTEM, ethnicity_code)))
    if date_of_birth:
      date_answers.append(("dateOfBirth", date_of_birth))
    if state:
      string_answers.append(("state", state))
    qr = make_questionnaire_response_json(participant_id,
                                          questionnaire_id,
                                          code_answers = code_answers,
                                          string_answers = string_answers,
                                          date_answers = date_answers)
    self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def _create_data(self):
    SqlTestBase.setup_codes(METRIC_FIELD_TO_QUESTION_CODE.values(),
                            code_type=CodeType.QUESTION)
    SqlTestBase.setup_codes(FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values(),
                            code_type=CodeType.MODULE)
    # Import codes for white, female, and hispanic, but not male or black.
    SqlTestBase.setup_codes(["white", "female", "hispanic"], code_type=CodeType.ANSWER)
    participant_dao = ParticipantDao()

    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    questionnaire_id_2 = self.create_questionnaire('questionnaire4.json')
    with FakeClock(TIME):
      participant = Participant(participantId=1, biobankId=2)
      participant_dao.insert(participant)

    with FakeClock(TIME):
      participant2 = Participant(participantId=2, biobankId=3,
                                 providerLink=primary_provider_link('PITT'))
      participant_dao.insert(participant2)

    with FakeClock(TIME_2):
      # This update to participant has no effect, as the HPO ID didn't change.
      participant = Participant(participantId=1, version=1, biobankId=2,
                                clientId='blah')
      participant_dao.update(participant)
      self.submit_questionnaire_response('P1', questionnaire_id, "white", "male",
                                         "hispanic", None, datetime.date(1980, 1, 2))
      self.submit_questionnaire_response('P2', questionnaire_id, None, None, None, None, None)

    with FakeClock(TIME_3):
      participant = Participant(participantId=1, version=2, biobankId=2, clientId='blah',
                                providerLink=primary_provider_link('PITT'))
      participant_dao.update(participant)
      self.send_post('Participant/P2/PhysicalMeasurements', load_measurement_json(2))
      self.send_post('Participant/P2/BiobankOrder', load_biobank_order_json(2))
      self.submit_questionnaire_response('P1', questionnaire_id, "black", "female",
                                         "hispanic", None, datetime.date(1980, 1, 3))
      self.submit_questionnaire_response('P2', questionnaire_id_2, None, None, None, 'VA', None)
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

    with FakeClock(TIME_3):
      MetricsExport.start_export_tasks(BUCKET_NAME, 2)
      run_deferred_tasks(self)

    t1 = TIME.strftime(TIME_FORMAT)
    t2 = TIME_2.strftime(TIME_FORMAT)
    t3 = TIME_3.strftime(TIME_FORMAT)
    prefix = TIME_3.isoformat() + "/"

    # Two shards are written for each file, one with the first participant and
    # one with the second.
    assertCsvContents(self, BUCKET_NAME, prefix + HPO_IDS_CSV % 0,
                      [HPO_ID_FIELDS,
                       ['2', 'PITT', t1]])
    assertCsvContents(self, BUCKET_NAME, prefix + HPO_IDS_CSV % 1,
                      [HPO_ID_FIELDS,
                       ['1', 'UNSET', t1],
                       ['1', 'PITT', t3]])
    participant_fields = get_participant_fields()
    assertCsvContents(self, BUCKET_NAME, prefix + PARTICIPANTS_CSV % 0,
                      [participant_fields,
                       ['2', '', '2016-01-04T09:40:21Z', '', t3, t3, t3, t2]])
    assertCsvContents(self, BUCKET_NAME, prefix + PARTICIPANTS_CSV % 1,
                      [participant_fields,
                       ['1', '1980-01-03', '', t2, '', '', '', t2]])
    assertCsvContents(self, BUCKET_NAME, prefix + ANSWERS_CSV % 0,
                      [ANSWER_FIELDS,
                      ['2', t3, STATE_QUESTION_CODE, '', 'VA']])
    assertCsvContents(self, BUCKET_NAME, prefix + ANSWERS_CSV % 1,
                      [ANSWER_FIELDS,
                       ['1', t2, GENDER_IDENTITY_QUESTION_CODE, 'UNMAPPED', ''],
                       ['1', t2, RACE_QUESTION_CODE, 'white', ''],
                       ['1', t2, ETHNICITY_QUESTION_CODE, 'hispanic', ''],
                       ['1', t3, GENDER_IDENTITY_QUESTION_CODE, 'female', ''],
                       ['1', t3, RACE_QUESTION_CODE, 'UNMAPPED', ''],
                       ['1', t3, ETHNICITY_QUESTION_CODE, 'hispanic', '']])

    # Wait for the metrics pipeline to run, processing the CSV output.
    with FakeClock(TIME_4):
      test_support.execute_until_empty(self.taskqueue)

    metrics_version = MetricsVersionDao().get_serving_version()
    expected_version = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True,
                                      date=TIME_4, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_version.asdict(), metrics_version.asdict())

    buckets = MetricsVersionDao().get_with_children(metrics_version.metricsVersionId).buckets
    bucket_map = {(bucket.date, bucket.hpoId): bucket for bucket in buckets}
    # At TIME, P1 has no HPO and P2 has PITT.
    self.assertBucket(bucket_map, TIME, 'UNSET',
                      { 'Participant': 1,
                        'Participant.ageRange.26-35': 1,
                        'Participant.censusRegion.UNSET': 1,
                        'Participant.physicalMeasurements.UNSET': 1,
                        'Participant.biospecimen.UNSET': 1,
                        'Participant.biospecimenSamples.UNSET': 1,
                        'Participant.hpoId.UNSET': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 1,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 1,
                        'Participant.questionnaireOnSociodemographics.UNSET': 1,
                        'Participant.genderIdentity.UNSET': 1,
                        'Participant.race.UNSET': 1,
                        'Participant.ethnicity.UNSET': 1,
                        'Participant.biospecimenSummary.UNSET': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1 })
    self.assertBucket(bucket_map, TIME, 'PITT',
                      { 'Participant': 1,
                        'Participant.ageRange.UNSET': 1,
                        'Participant.censusRegion.UNSET': 1,
                        'Participant.physicalMeasurements.UNSET': 1,
                        'Participant.biospecimen.UNSET': 1,
                        'Participant.biospecimenSamples.UNSET': 1,
                        'Participant.hpoId.PITT': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 1,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 1,
                        'Participant.questionnaireOnSociodemographics.UNSET': 1,
                        'Participant.genderIdentity.UNSET': 1,
                        'Participant.race.UNSET': 1,
                        'Participant.ethnicity.UNSET': 1,
                        'Participant.biospecimenSummary.UNSET': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1 })
    self.assertBucket(bucket_map, TIME, '',
                      { 'Participant': 2,
                        'Participant.ageRange.26-35': 1,
                        'Participant.ageRange.UNSET': 1,
                        'Participant.censusRegion.UNSET': 2,
                        'Participant.physicalMeasurements.UNSET': 2,
                        'Participant.biospecimen.UNSET': 2,
                        'Participant.biospecimenSamples.UNSET': 2,
                        'Participant.hpoId.PITT': 1,
                        'Participant.hpoId.UNSET': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 2,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 2,
                        'Participant.questionnaireOnSociodemographics.UNSET': 2,
                        'Participant.genderIdentity.UNSET': 2,
                        'Participant.race.UNSET': 2,
                        'Participant.ethnicity.UNSET': 2,
                        'Participant.biospecimenSummary.UNSET': 2,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 2 })
    # At TIME_2, P1 is white, UNMAPPED gender and hispanic ethnicity; biobank samples
    # arrived for P1; and both participants have submitted the sociodemographics questionnaire.
    self.assertBucket(bucket_map, TIME_2, 'UNSET',
                      { 'Participant': 1,
                        'Participant.ageRange.26-35': 1,
                        'Participant.censusRegion.UNSET': 1,
                        'Participant.physicalMeasurements.UNSET': 1,
                        'Participant.biospecimen.UNSET': 1,
                        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 1,
                        'Participant.hpoId.UNSET': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 1,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 1,
                        'Participant.questionnaireOnSociodemographics.SUBMITTED': 1,
                        'Participant.genderIdentity.UNMAPPED': 1,
                        'Participant.race.white': 1,
                        'Participant.ethnicity.hispanic': 1,
                        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1 })
    self.assertBucket(bucket_map, TIME_2, 'PITT',
                      { 'Participant': 1,
                        'Participant.ageRange.UNSET': 1,
                        'Participant.censusRegion.UNSET': 1,
                        'Participant.physicalMeasurements.UNSET': 1,
                        'Participant.biospecimen.UNSET': 1,
                        'Participant.biospecimenSamples.UNSET': 1,
                        'Participant.hpoId.PITT': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 1,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 1,
                        'Participant.questionnaireOnSociodemographics.SUBMITTED': 1,
                        'Participant.genderIdentity.UNSET': 1,
                        'Participant.race.UNSET': 1,
                        'Participant.ethnicity.UNSET': 1,
                        'Participant.biospecimenSummary.UNSET': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1 })
    self.assertBucket(bucket_map, TIME_2, '',
                      { 'Participant': 2,
                        'Participant.ageRange.26-35': 1,
                        'Participant.ageRange.UNSET': 1,
                        'Participant.censusRegion.UNSET': 2,
                        'Participant.physicalMeasurements.UNSET': 2,
                        'Participant.biospecimen.UNSET': 2,
                        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 1,
                        'Participant.biospecimenSamples.UNSET': 1,
                        'Participant.hpoId.PITT': 1,
                        'Participant.hpoId.UNSET': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 2,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 2,
                        'Participant.questionnaireOnSociodemographics.SUBMITTED': 2,
                        'Participant.genderIdentity.UNMAPPED': 1,
                        'Participant.genderIdentity.UNSET': 1,
                        'Participant.race.white': 1,
                        'Participant.race.UNSET': 1,
                        'Participant.ethnicity.hispanic': 1,
                        'Participant.ethnicity.UNSET': 1,
                        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 1,
                        'Participant.biospecimenSummary.UNSET': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 2 })
    # At TIME_3, P1 is UNMAPPED race, female gender, and now in PITT HPO;
    # physical measurements and a questionnaire for personal
    # habits and overall health are submitted for P2, and P2 is in SOUTH census region
    # and in a new age bucket (since it was their birthday.)
    self.assertBucket(bucket_map, TIME_3, 'UNSET')
    self.assertBucket(bucket_map, TIME_3, 'PITT',
                      { 'Participant': 2,
                        'Participant.ageRange.36-45': 1,
                        'Participant.ageRange.UNSET': 1,
                        'Participant.censusRegion.SOUTH': 1,
                        'Participant.censusRegion.UNSET': 1,
                        'Participant.physicalMeasurements.COMPLETE': 1,
                        'Participant.physicalMeasurements.UNSET': 1,
                        'Participant.biospecimen.UNSET': 2,
                        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 1,
                        'Participant.biospecimenSamples.UNSET': 1,
                        'Participant.hpoId.PITT': 2,
                        'Participant.questionnaireOnOverallHealth.SUBMITTED': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 1,
                        'Participant.questionnaireOnPersonalHabits.SUBMITTED': 1,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 1,
                        'Participant.questionnaireOnSociodemographics.SUBMITTED': 2,
                        'Participant.genderIdentity.female': 1,
                        'Participant.genderIdentity.UNSET': 1,
                        'Participant.race.UNMAPPED': 1,
                        'Participant.race.UNSET': 1,
                        'Participant.ethnicity.hispanic': 1,
                        'Participant.ethnicity.UNSET': 1,
                        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 1,
                        'Participant.biospecimenSummary.UNSET': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 2 })
    self.assertBucket(bucket_map, TIME_3, '',
                      { 'Participant': 2,
                        'Participant.ageRange.36-45': 1,
                        'Participant.ageRange.UNSET': 1,
                        'Participant.censusRegion.SOUTH': 1,
                        'Participant.censusRegion.UNSET': 1,
                        'Participant.physicalMeasurements.COMPLETE': 1,
                        'Participant.physicalMeasurements.UNSET': 1,
                        'Participant.biospecimen.UNSET': 2,
                        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 1,
                        'Participant.biospecimenSamples.UNSET': 1,
                        'Participant.hpoId.PITT': 2,
                        'Participant.questionnaireOnOverallHealth.SUBMITTED': 1,
                        'Participant.questionnaireOnOverallHealth.UNSET': 1,
                        'Participant.questionnaireOnPersonalHabits.SUBMITTED': 1,
                        'Participant.questionnaireOnPersonalHabits.UNSET': 1,
                        'Participant.questionnaireOnSociodemographics.SUBMITTED': 2,
                        'Participant.genderIdentity.female': 1,
                        'Participant.genderIdentity.UNSET': 1,
                        'Participant.race.UNMAPPED': 1,
                        'Participant.race.UNSET': 1,
                        'Participant.ethnicity.hispanic': 1,
                        'Participant.ethnicity.UNSET': 1,
                        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 1,
                        'Participant.biospecimenSummary.UNSET': 1,
                        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 2 })
    # There si a biobank order on 1/4, but it gets ignored since it's after the run date.
    self.assertBucket(bucket_map, TIME_4, '')

  def assertBucket(self, bucket_map, dt, hpoId, metrics=None):
    bucket = bucket_map.get((dt.date(), hpoId))
    if metrics:
      self.assertIsNotNone(bucket)
      self.assertMultiLineEqual(pretty(metrics),
                                pretty(json.loads(bucket.metrics)))
    else:
      self.assertIsNone(bucket)
