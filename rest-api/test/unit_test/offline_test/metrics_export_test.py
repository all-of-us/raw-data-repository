from __future__ import print_function

import datetime
import json
import offline.metrics_export

from clock import FakeClock
from code_constants import CONSENT_PERMISSION_YES_CODE, CONSENT_PERMISSION_NO_CODE
from code_constants import GENDER_IDENTITY_QUESTION_CODE, EHR_CONSENT_QUESTION_CODE
from code_constants import RACE_QUESTION_CODE, STATE_QUESTION_CODE, RACE_WHITE_CODE
from code_constants import RACE_NONE_OF_THESE_CODE, PMI_PREFER_NOT_TO_ANSWER_CODE, PMI_SKIP_CODE
from participant_enums import WithdrawalStatus, make_primary_provider_link_for_name
from field_mappings import FIELD_TO_QUESTIONNAIRE_MODULE_CODE
from mapreduce import test_support
from model.biobank_stored_sample import BiobankStoredSample
from model.code import CodeType
from model.hpo import HPO
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.hpo_dao import HPODao
from dao.metrics_dao import MetricsVersionDao, SERVING_METRICS_DATA_VERSION
from dao.participant_dao import ParticipantDao
from model.metrics import MetricsVersion
from model.participant import Participant
from offline.metrics_config import ANSWER_FIELD_TO_QUESTION_CODE
from offline.metrics_config import get_participant_fields, HPO_ID_FIELDS, ANSWER_FIELDS
from offline.metrics_export import MetricsExport, _HPO_IDS_CSV, _PARTICIPANTS_CSV, _ANSWERS_CSV
from offline_test.gcs_utils import assertCsvContents
from test_data import load_biobank_order_json, load_measurement_json
from unit_test_util import FlaskTestBase, CloudStorageSqlTestBase, SqlTestBase, TestBase
from unit_test_util import make_questionnaire_response_json, pretty, run_deferred_tasks
from unit_test_util import PITT_HPO_ID

BUCKET_NAME = 'pmi-drc-biobank-test.appspot.com'
TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class MetricsExportTest(CloudStorageSqlTestBase, FlaskTestBase):
  """Tests exporting of metrics AND the output of the metrics pipeline that runs when the

   export completes.
  """
  assertCsvContents = assertCsvContents

  def setUp(self):
    super(MetricsExportTest, self).setUp()
    FlaskTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    offline.metrics_export._QUEUE_NAME = 'default'
    self.taskqueue.FlushQueue('default')
    self.maxDiff = None

  def tearDown(self):
    super(MetricsExportTest, self).tearDown()
    FlaskTestBase.doTearDown(self)

  def _submit_empty_questionnaire_response(self, participant_id,
                                           questionnaire_id):
    qr = make_questionnaire_response_json(participant_id, questionnaire_id)
    self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def _create_data(self):
    HPODao().insert(HPO(hpoId=PITT_HPO_ID + 1, name='AZ_TUCSON_2'))
    HPODao().insert(HPO(hpoId=PITT_HPO_ID + 4, name='TEST'))
    SqlTestBase.setup_codes(
        ANSWER_FIELD_TO_QUESTION_CODE.values() + [EHR_CONSENT_QUESTION_CODE],
        code_type=CodeType.QUESTION)
    SqlTestBase.setup_codes(
        FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values(), code_type=CodeType.MODULE)

    # Import codes for white and female, but not male or black.
    SqlTestBase.setup_codes([
      RACE_WHITE_CODE, CONSENT_PERMISSION_YES_CODE, RACE_NONE_OF_THESE_CODE,
      PMI_PREFER_NOT_TO_ANSWER_CODE, CONSENT_PERMISSION_NO_CODE, 'female', 'PIIState_VA',
      PMI_SKIP_CODE
    ], code_type=CodeType.ANSWER)
    participant_dao = ParticipantDao()

    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    questionnaire_id_2 = self.create_questionnaire('questionnaire4.json')
    questionnaire_id_3 = self.create_questionnaire('all_consents_questionnaire.json')

    pl_tucson = make_primary_provider_link_for_name('AZ_TUCSON')
    pl_test = make_primary_provider_link_for_name('TEST')
    pl_pitt = make_primary_provider_link_for_name('PITT')

    with FakeClock(TIME):
      participant = Participant(participantId=1, biobankId=2, providerLink=pl_tucson)
      participant_dao.insert(participant)
      self.send_consent('P1', email='bob@gmail.com')

      # Participant 2 starts out unpaired; later gets paired automatically when their physical
      # measurements come in.
      participant2 = Participant(participantId=2, biobankId=3)
      participant_dao.insert(participant2)
      self.send_consent('P2', email='bob@fexample.com')

      # Test HPO affiliation; this test participant is ignored.
      participant3 = Participant(participantId=3, biobankId=4, providerLink=pl_test)
      participant_dao.insert(participant3)
      self.send_consent('P3', email='fred@gmail.com')

      # example.com e-mail; this test participant is ignored, too.
      participant4 = Participant(participantId=4, biobankId=5, providerLink=pl_pitt)
      participant_dao.insert(participant4)
      self.send_consent('P4', email='bob@example.com')

      participant5 = Participant(participantId=5, biobankId=6, providerLink=pl_tucson)
      participant_dao.insert(participant5)
      self.send_consent('P5', email='larry@gmail.com')

      participant6 = Participant(participantId=6, biobankId=7, providerLink=pl_tucson)
      participant_dao.insert(participant6)
      self.send_consent('P6', email='larry@gmail.com')

      # Participant that starts at PITT but winds up in TEST; should be ignored.
      participant7 = Participant(participantId=7, biobankId=8, providerLink=pl_pitt)
      participant_dao.insert(participant7)
      self.send_consent('P7', email='larry@gmail.com')

      # Participant that re-pairs and then withdraws; should be ignored.
      participant8 = Participant(participantId=8, biobankId=9, providerLink=pl_pitt)
      participant_dao.insert(participant8)
      self.send_consent('P8', email='larry@gmail.com')

    with FakeClock(TIME_2):
      # FIXME: The test passes, but the following "update" doesn't actually make much sense.  The
      # providerlink is not changed but the HPO ID actually is (at this point in time
      # `participant.hpoId` evaluates to 4, which is the value given in `unit_test_util.AZ_HPO_ID`).
      # The original intent of the test is not clear.
      # This update to participant has no effect, as the HPO ID didn't change.
      participant = self._participant_with_defaults(
        participantId=1, version=1, biobankId=2,
        hpoId=3, # <<<< Had to add hpoId here, default is UNSET_HPO_ID
        providerLink=pl_tucson
      )
      participant_dao.update(participant)

      participant8.providerLink = pl_tucson
      participant_dao.update(participant8)

      self.submit_questionnaire_response('P1', questionnaire_id,
                                         race_code=RACE_WHITE_CODE,
                                         gender_code='male',
                                         state=PMI_SKIP_CODE,
                                         date_of_birth=datetime.date(1980, 1, 2))

      self.submit_questionnaire_response('P2', questionnaire_id,
                                         race_code=RACE_NONE_OF_THESE_CODE,
                                         gender_code=None,
                                         state=None,
                                         date_of_birth=None)

      self.submit_questionnaire_response('P5', questionnaire_id,
                                         race_code=PMI_SKIP_CODE,
                                         gender_code=PMI_SKIP_CODE,
                                         state=None,
                                         date_of_birth=None)

      self.submit_questionnaire_response('P6', questionnaire_id,
                                         race_code=PMI_SKIP_CODE,
                                         gender_code=PMI_SKIP_CODE,
                                         state=None,
                                         date_of_birth=None)

    with FakeClock(TIME_3):
      t3 = TIME_3.strftime(TIME_FORMAT)
      # Re-pair the original participant
      participant.version = 2
      participant.providerLink = pl_pitt
      participant_dao.update(participant)

      participant7.providerLink = pl_test
      participant_dao.update(participant7)

      participant8.withdrawalStatus = WithdrawalStatus.NO_USE
      participant_dao.update(participant8)

      self.send_post('Participant/P2/PhysicalMeasurements', load_measurement_json(2, t3))
      self.send_post('Participant/P2/BiobankOrder', load_biobank_order_json(2))

      self.submit_questionnaire_response('P1', questionnaire_id,
                                         race_code='black',
                                         gender_code='female',
                                         state=None,
                                         date_of_birth=datetime.date(1980, 1, 3))

      self.submit_questionnaire_response('P2', questionnaire_id,
                                         race_code=None,
                                         gender_code=PMI_PREFER_NOT_TO_ANSWER_CODE,
                                         state=None,
                                         date_of_birth=None)

      self.submit_questionnaire_response('P2', questionnaire_id_2,
                                         race_code=None,
                                         gender_code=None,
                                         state='PIIState_VA',
                                         date_of_birth=None)
      self.submit_questionnaire_response('P6', questionnaire_id_2,
                                         race_code=None,
                                         gender_code=None,
                                         state='PIIState_VA',
                                         date_of_birth=None)

      self.submit_consent_questionnaire_response('P1', questionnaire_id_3,
                                                  CONSENT_PERMISSION_NO_CODE)

      self.submit_consent_questionnaire_response('P2', questionnaire_id_3,
                                                  CONSENT_PERMISSION_YES_CODE)

      self.submit_consent_questionnaire_response('P6', questionnaire_id_3,
                                                 CONSENT_PERMISSION_YES_CODE)

      sample_dao = BiobankStoredSampleDao()
      sample_dao.insert(
          BiobankStoredSample(
              biobankStoredSampleId='abc',
              biobankId=2,
              biobankOrderIdentifier='KIT',
              test='test',
              confirmed=TIME_2))
      sample_dao.insert(
          BiobankStoredSample(
              biobankStoredSampleId='def',
              biobankId=3,
              biobankOrderIdentifier='KIT',
              test='1SAL',
              confirmed=TIME_2))
      sample_dao.insert(
          BiobankStoredSample(
              biobankStoredSampleId='xyz',
              biobankId=4,
              biobankOrderIdentifier='KIT',
              test='1SAL',
              confirmed=TIME_2))

      # participant 6 withdrawl shouldn't be in csv's.
      participant6.withdrawalStatus = WithdrawalStatus.NO_USE
      participant_dao.update(participant6)

  def test_metric_export(self):
    self._create_data()

    with FakeClock(TIME_3):
      MetricsExport.start_export_tasks(BUCKET_NAME, 2)
      run_deferred_tasks(self)

    t1 = TIME.strftime(TIME_FORMAT)
    t2 = TIME_2.strftime(TIME_FORMAT)
    t3 = TIME_3.strftime(TIME_FORMAT)
    prefix = TIME_3.isoformat() + '/'

    # Two shards are written for each file, one with the first participant and
    # one with the second.
    self.assertCsvContents(BUCKET_NAME, prefix + _HPO_IDS_CSV % 0, [
      HPO_ID_FIELDS,
      ['2', 'UNSET', t1],
      ['2', 'PITT', t3],
    ])

    self.assertCsvContents(BUCKET_NAME, prefix + _HPO_IDS_CSV % 1, [
      HPO_ID_FIELDS,
      ['1', 'AZ_TUCSON', t1],
      ['1', 'PITT', t3],
      # See the FIXME above about this row of the CSV
      ['1', 'AZ_TUCSON_2', t2],
      ['5', 'AZ_TUCSON', t1],
    ])

    participant_fields = get_participant_fields()
    #  The participant fields are as follows:
    # ['participant_id',
    #  'date_of_birth',
    #  'first_order_date',
    #  'first_samples_arrived_date',
    #  'first_physical_measurements_date',
    #  'first_samples_to_isolate_dna_date',
    #  'consent_for_study_enrollment_time',
    #  'questionnaire_on_family_health_time',
    #  'questionnaire_on_healthcare_access_time',
    #  'questionnaire_on_lifestyle_time',
    #  'questionnaire_on_medical_history_time',
    #  'questionnaire_on_medications_time',
    #  'questionnaire_on_overall_health_time',
    #  'questionnaire_on_the_basics_time']]

    self.assertCsvContents(BUCKET_NAME, prefix + _PARTICIPANTS_CSV % 0, [
      participant_fields,
      ['2', '', '2016-01-04T09:40:21Z', t2, t3, t2, t1, '', '', t3, '', '', t3, t2]
    ])

    self.assertCsvContents(BUCKET_NAME, prefix + _PARTICIPANTS_CSV % 1, [
      participant_fields,
      ['1', '1980-01-03', '', t2, '', '', t1, '', '', '', '', '', '', t2],
      ['5', '', '', '', '', '', t1, '', '', '', '', '', '', t2]
    ])

    self.assertCsvContents(BUCKET_NAME, prefix + _ANSWERS_CSV % 0, [
      ANSWER_FIELDS,
      ['2', t3, STATE_QUESTION_CODE, 'PIIState_VA', ''],
      ['2', t3, EHR_CONSENT_QUESTION_CODE, CONSENT_PERMISSION_YES_CODE, ''],
      ['2', t2, RACE_QUESTION_CODE, RACE_NONE_OF_THESE_CODE, ''],
      ['2', t3, GENDER_IDENTITY_QUESTION_CODE, PMI_PREFER_NOT_TO_ANSWER_CODE, ''],
    ])

    self.assertCsvContents(BUCKET_NAME, prefix + _ANSWERS_CSV % 1, [
      ANSWER_FIELDS,
      ['1', t2, GENDER_IDENTITY_QUESTION_CODE, 'UNMAPPED', ''],
      ['1', t2, RACE_QUESTION_CODE, RACE_WHITE_CODE, ''],
      ['1', t3, GENDER_IDENTITY_QUESTION_CODE, 'female', ''],
      ['1', t3, RACE_QUESTION_CODE, 'UNMAPPED', ''],
      ['1', t3, EHR_CONSENT_QUESTION_CODE, CONSENT_PERMISSION_NO_CODE, ''],
      ['1', t2, STATE_QUESTION_CODE, PMI_SKIP_CODE, ''],
      ['5', t2, GENDER_IDENTITY_QUESTION_CODE, PMI_SKIP_CODE, ''],
      ['5', t2, RACE_QUESTION_CODE, PMI_SKIP_CODE, ''],
    ])

    # Wait for the metrics pipeline to run, processing the CSV output.
    with FakeClock(TIME_4):
      test_support.execute_until_empty(self.taskqueue)

    metrics_version = MetricsVersionDao().get_serving_version()
    expected_version = MetricsVersion(
        metricsVersionId=1,
        inProgress=False,
        complete=True,
        date=TIME_4,
        dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_version.asdict(), metrics_version.asdict())

    buckets = MetricsVersionDao().get_with_children(metrics_version.metricsVersionId).buckets
    bucket_map = {(bucket.date, bucket.hpoId): bucket for bucket in buckets}

    # N.B: If these data dicts are kept sorted, it is significantly easier both to read the source
    # code and to compare the code with the output of any failures in this test.
    # PLEASE keep these sorted.

    # At TIME, P1 and P5 are affiliated with AZ_TUCSON and P2 is UNSET.
    self.assertBucket(bucket_map, TIME, 'TEST')
    self.assertBucket(bucket_map, TIME, 'AZ_TUCSON', {
        'Participant': 2,
        'Participant.ageRange.26-35': 1,
        'Participant.ageRange.UNSET': 1,
        'Participant.biospecimen.UNSET': 2,
        'Participant.biospecimenSamples.UNSET': 2,
        'Participant.biospecimenSummary.UNSET': 2,
        'Participant.censusRegion.UNSET': 2,
        'Participant.consentForElectronicHealthRecords.UNSET': 2,
        'Participant.consentForStudyEnrollment.SUBMITTED': 2,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 2,
        'Participant.enrollmentStatus.INTERESTED': 2,
        'Participant.genderIdentity.UNSET': 2,
        'Participant.hpoId.AZ_TUCSON': 2,
        'Participant.numCompletedBaselinePPIModules.0': 2,
        'Participant.physicalMeasurements.UNSET': 2,
        'Participant.questionnaireOnFamilyHealth.UNSET': 2,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 2,
        'Participant.questionnaireOnLifestyle.UNSET': 2,
        'Participant.questionnaireOnMedicalHistory.UNSET': 2,
        'Participant.questionnaireOnMedications.UNSET': 2,
        'Participant.questionnaireOnOverallHealth.UNSET': 2,
        'Participant.questionnaireOnTheBasics.UNSET': 2,
        'Participant.race.UNSET': 2,
        'Participant.samplesToIsolateDNA.UNSET': 2,
        'Participant.state.UNSET': 2,
    })
    self.assertBucket(bucket_map, TIME, 'UNSET', {
        'Participant': 1,
        'Participant.ageRange.UNSET': 1,
        'Participant.biospecimen.UNSET': 1,
        'Participant.biospecimenSamples.UNSET': 1,
        'Participant.biospecimenSummary.UNSET': 1,
        'Participant.censusRegion.UNSET': 1,
        'Participant.consentForElectronicHealthRecords.UNSET': 1,
        'Participant.consentForStudyEnrollment.SUBMITTED': 1,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1,
        'Participant.enrollmentStatus.INTERESTED': 1,
        'Participant.genderIdentity.UNSET': 1,
        'Participant.hpoId.UNSET': 1,
        'Participant.numCompletedBaselinePPIModules.0': 1,
        'Participant.physicalMeasurements.UNSET': 1,
        'Participant.questionnaireOnFamilyHealth.UNSET': 1,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 1,
        'Participant.questionnaireOnLifestyle.UNSET': 1,
        'Participant.questionnaireOnMedicalHistory.UNSET': 1,
        'Participant.questionnaireOnMedications.UNSET': 1,
        'Participant.questionnaireOnOverallHealth.UNSET': 1,
        'Participant.questionnaireOnTheBasics.UNSET': 1,
        'Participant.race.UNSET': 1,
        'Participant.samplesToIsolateDNA.UNSET': 1,
        'Participant.state.UNSET': 1,
    })
    self.assertBucket(bucket_map, TIME, '', {
        'Participant': 3,
        'Participant.ageRange.26-35': 1,
        'Participant.ageRange.UNSET': 2,
        'Participant.biospecimen.UNSET': 3,
        'Participant.biospecimenSamples.UNSET': 3,
        'Participant.biospecimenSummary.UNSET': 3,
        'Participant.censusRegion.UNSET': 3,
        'Participant.consentForElectronicHealthRecords.UNSET': 3,
        'Participant.consentForStudyEnrollment.SUBMITTED': 3,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 3,
        'Participant.enrollmentStatus.INTERESTED': 3,
        'Participant.genderIdentity.UNSET': 3,
        'Participant.hpoId.AZ_TUCSON': 2,
        'Participant.hpoId.UNSET': 1,
        'Participant.numCompletedBaselinePPIModules.0': 3,
        'Participant.physicalMeasurements.UNSET': 3,
        'Participant.questionnaireOnFamilyHealth.UNSET': 3,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 3,
        'Participant.questionnaireOnLifestyle.UNSET': 3,
        'Participant.questionnaireOnMedicalHistory.UNSET': 3,
        'Participant.questionnaireOnMedications.UNSET': 3,
        'Participant.questionnaireOnOverallHealth.UNSET': 3,
        'Participant.questionnaireOnTheBasics.UNSET': 3,
        'Participant.race.UNSET': 3,
        'Participant.samplesToIsolateDNA.UNSET': 3,
        'Participant.state.UNSET': 3,
    })

    # At TIME_2, P1 is white, UNMAPPED gender; biobank samples
    # arrived for P1 and P2 (the latter updating samplesToIsolateDNA);
    # and both participants have submitted the basics questionnaire.
    self.assertBucket(bucket_map, TIME_2, 'AZ_TUCSON_2', {
        'Participant': 1,
        'Participant.ageRange.26-35': 1,
        'Participant.biospecimen.UNSET': 1,
        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 1,
        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 1,
        'Participant.censusRegion.PMI_Skip': 1,
        'Participant.consentForElectronicHealthRecords.UNSET': 1,
        'Participant.consentForStudyEnrollment.SUBMITTED': 1,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1,
        'Participant.enrollmentStatus.INTERESTED': 1,
        'Participant.genderIdentity.UNMAPPED': 1,
        'Participant.hpoId.AZ_TUCSON_2': 1,
        'Participant.numCompletedBaselinePPIModules.1': 1,
        'Participant.physicalMeasurements.UNSET': 1,
        'Participant.questionnaireOnFamilyHealth.UNSET': 1,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 1,
        'Participant.questionnaireOnLifestyle.UNSET': 1,
        'Participant.questionnaireOnMedicalHistory.UNSET': 1,
        'Participant.questionnaireOnMedications.UNSET': 1,
        'Participant.questionnaireOnOverallHealth.UNSET': 1,
        'Participant.questionnaireOnTheBasics.SUBMITTED': 1,
        'Participant.race.WHITE': 1,
        'Participant.samplesToIsolateDNA.UNSET': 1,
        'Participant.state.PMI_Skip': 1,
    })
    self.assertBucket(bucket_map, TIME_2, 'UNSET', {
        'Participant': 1,
        'Participant.ageRange.UNSET': 1,
        'Participant.biospecimen.UNSET': 1,
        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 1,
        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 1,
        'Participant.censusRegion.UNSET': 1,
        'Participant.consentForElectronicHealthRecords.UNSET': 1,
        'Participant.consentForStudyEnrollment.SUBMITTED': 1,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1,
        'Participant.enrollmentStatus.INTERESTED': 1,
        'Participant.genderIdentity.UNSET': 1,
        'Participant.hpoId.UNSET': 1,
        'Participant.numCompletedBaselinePPIModules.1': 1,
        'Participant.physicalMeasurements.UNSET': 1,
        'Participant.questionnaireOnFamilyHealth.UNSET': 1,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 1,
        'Participant.questionnaireOnLifestyle.UNSET': 1,
        'Participant.questionnaireOnMedicalHistory.UNSET': 1,
        'Participant.questionnaireOnMedications.UNSET': 1,
        'Participant.questionnaireOnOverallHealth.UNSET': 1,
        'Participant.questionnaireOnTheBasics.SUBMITTED': 1,
        'Participant.race.OTHER_RACE': 1,
        'Participant.samplesToIsolateDNA.RECEIVED': 1,
        'Participant.state.UNSET': 1,
    })
    self.assertBucket(bucket_map, TIME_2, '', {
        'Participant': 3,
        'Participant.ageRange.26-35': 1,
        'Participant.ageRange.UNSET': 2,
        'Participant.biospecimen.UNSET': 3,
        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 2,
        'Participant.biospecimenSamples.UNSET': 1,
        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 2,
        'Participant.biospecimenSummary.UNSET': 1,
        'Participant.censusRegion.PMI_Skip': 1,
        'Participant.censusRegion.UNSET': 2,
        'Participant.consentForElectronicHealthRecords.UNSET': 3,
        'Participant.consentForStudyEnrollment.SUBMITTED': 3,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 3,
        'Participant.enrollmentStatus.INTERESTED': 3,
        'Participant.genderIdentity.PMI_Skip': 1,
        'Participant.genderIdentity.UNMAPPED': 1,
        'Participant.genderIdentity.UNSET': 1,
        'Participant.hpoId.AZ_TUCSON': 1,
        'Participant.hpoId.AZ_TUCSON_2': 1,
        'Participant.hpoId.UNSET': 1,
        'Participant.numCompletedBaselinePPIModules.1': 3,
        'Participant.physicalMeasurements.UNSET': 3,
        'Participant.questionnaireOnFamilyHealth.UNSET': 3,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 3,
        'Participant.questionnaireOnLifestyle.UNSET': 3,
        'Participant.questionnaireOnMedicalHistory.UNSET': 3,
        'Participant.questionnaireOnMedications.UNSET': 3,
        'Participant.questionnaireOnOverallHealth.UNSET': 3,
        'Participant.questionnaireOnTheBasics.SUBMITTED': 3,
        'Participant.race.OTHER_RACE': 1,
        'Participant.race.PMI_Skip': 1,
        'Participant.race.WHITE': 1,
        'Participant.samplesToIsolateDNA.RECEIVED': 1,
        'Participant.samplesToIsolateDNA.UNSET': 2,
        'Participant.state.PMI_Skip': 1,
        'Participant.state.UNSET': 2,
    })

    # At TIME_3, P1 is UNSET race, UNMAPPED female gender, and now in PITT HPO;
    # physical measurements and a questionnaire for personal
    # habits and overall health are submitted for P2, both participants submit consent
    # questionnaires, with P1 not consenting to EHR; and P2 is in SOUTH census region
    # and in a new age bucket (since it was their birthday.)
    # P2 now has an enrollment status of FULL_MEMBER, and P1 has MEMBER
    # Therefore, only P5 should show up here
    self.assertBucket(bucket_map, TIME_3, 'AZ_TUCSON', {
        'Participant': 1,
        'Participant.ageRange.UNSET': 1,
        'Participant.biospecimen.UNSET': 1,
        'Participant.biospecimenSamples.UNSET': 1,
        'Participant.biospecimenSummary.UNSET': 1,
        'Participant.censusRegion.UNSET': 1,
        'Participant.consentForElectronicHealthRecords.UNSET': 1,
        'Participant.consentForStudyEnrollment.SUBMITTED': 1,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1,
        'Participant.enrollmentStatus.INTERESTED': 1,
        'Participant.genderIdentity.PMI_Skip': 1,
        'Participant.hpoId.AZ_TUCSON': 1,
        'Participant.numCompletedBaselinePPIModules.1': 1,
        'Participant.physicalMeasurements.UNSET': 1,
        'Participant.questionnaireOnFamilyHealth.UNSET': 1,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 1,
        'Participant.questionnaireOnLifestyle.UNSET': 1,
        'Participant.questionnaireOnMedicalHistory.UNSET': 1,
        'Participant.questionnaireOnMedications.UNSET': 1,
        'Participant.questionnaireOnOverallHealth.UNSET': 1,
        'Participant.questionnaireOnTheBasics.SUBMITTED': 1,
        'Participant.race.PMI_Skip': 1,
        'Participant.samplesToIsolateDNA.UNSET': 1,
        'Participant.state.UNSET': 1,
    })

    self.assertBucket(bucket_map, TIME_3, 'PITT', {
        'Participant': 2,
        'Participant.ageRange.36-45': 1,
        'Participant.ageRange.UNSET': 1,
        'Participant.biospecimen.UNSET': 2,
        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 2,
        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 2,
        'Participant.censusRegion.PMI_Skip': 1,
        'Participant.censusRegion.SOUTH': 1,
        'Participant.consentForElectronicHealthRecords.SUBMITTED': 1,
        'Participant.consentForElectronicHealthRecords.SUBMITTED_NO_CONSENT': 1,
        'Participant.consentForStudyEnrollment.SUBMITTED': 2,
        'Participant.consentForStudyEnrollmentAndEHR.SUBMITTED': 1,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 1,
        'Participant.enrollmentStatus.FULL_PARTICIPANT': 1,
        'Participant.enrollmentStatus.INTERESTED': 1,
        'Participant.genderIdentity.PMI_PreferNotToAnswer': 1,
        'Participant.genderIdentity.female': 1,
        'Participant.hpoId.PITT': 2,
        'Participant.numCompletedBaselinePPIModules.1': 1,
        'Participant.numCompletedBaselinePPIModules.3': 1,
        'Participant.physicalMeasurements.COMPLETED': 1,
        'Participant.physicalMeasurements.UNSET': 1,
        'Participant.questionnaireOnFamilyHealth.UNSET': 2,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 2,
        'Participant.questionnaireOnLifestyle.SUBMITTED': 1,
        'Participant.questionnaireOnLifestyle.UNSET': 1,
        'Participant.questionnaireOnMedicalHistory.UNSET': 2,
        'Participant.questionnaireOnMedications.UNSET': 2,
        'Participant.questionnaireOnOverallHealth.SUBMITTED': 1,
        'Participant.questionnaireOnOverallHealth.UNSET': 1,
        'Participant.questionnaireOnTheBasics.SUBMITTED': 2,
        'Participant.race.OTHER_RACE': 1,
        'Participant.race.UNSET': 1,
        'Participant.samplesToIsolateDNA.RECEIVED': 1,
        'Participant.samplesToIsolateDNA.UNSET': 1,
        'Participant.state.PIIState_VA': 1,
        'Participant.state.PMI_Skip': 1,

        'FullParticipant.ageRange.UNSET': 1,
        'FullParticipant.biospecimen.UNSET': 1,
        'FullParticipant.biospecimenSamples.SAMPLES_ARRIVED': 1,
        'FullParticipant.biospecimenSummary.SAMPLES_ARRIVED': 1,
        'FullParticipant.censusRegion.SOUTH': 1,
        'FullParticipant.consentForElectronicHealthRecords.SUBMITTED': 1,
        'FullParticipant.consentForStudyEnrollment.SUBMITTED': 1,
        'FullParticipant.consentForStudyEnrollmentAndEHR.SUBMITTED': 1,
        'FullParticipant.enrollmentStatus.FULL_PARTICIPANT': 1,
        'FullParticipant.genderIdentity.PMI_PreferNotToAnswer': 1,
        'FullParticipant.hpoId.PITT': 1,
        'FullParticipant.numCompletedBaselinePPIModules.3': 1,
        'FullParticipant.physicalMeasurements.COMPLETED': 1,
        'FullParticipant.questionnaireOnFamilyHealth.UNSET': 1,
        'FullParticipant.questionnaireOnHealthcareAccess.UNSET': 1,
        'FullParticipant.questionnaireOnLifestyle.SUBMITTED': 1,
        'FullParticipant.questionnaireOnMedicalHistory.UNSET': 1,
        'FullParticipant.questionnaireOnMedications.UNSET': 1,
        'FullParticipant.questionnaireOnOverallHealth.SUBMITTED': 1,
        'FullParticipant.questionnaireOnTheBasics.SUBMITTED': 1,
        'FullParticipant.race.OTHER_RACE': 1,
        'FullParticipant.samplesToIsolateDNA.RECEIVED': 1,
        'FullParticipant.state.PIIState_VA': 1,
    })
    self.assertBucket(bucket_map, TIME_3, '', {
        'Participant': 3,
        'Participant.ageRange.36-45': 1,
        'Participant.ageRange.UNSET': 2,
        'Participant.biospecimen.UNSET': 3,
        'Participant.biospecimenSamples.SAMPLES_ARRIVED': 2,
        'Participant.biospecimenSamples.UNSET': 1,
        'Participant.biospecimenSummary.SAMPLES_ARRIVED': 2,
        'Participant.biospecimenSummary.UNSET': 1,
        'Participant.censusRegion.PMI_Skip': 1,
        'Participant.censusRegion.SOUTH': 1,
        'Participant.censusRegion.UNSET': 1,
        'Participant.consentForElectronicHealthRecords.SUBMITTED': 1,
        'Participant.consentForElectronicHealthRecords.SUBMITTED_NO_CONSENT': 1,
        'Participant.consentForElectronicHealthRecords.UNSET': 1,
        'Participant.consentForStudyEnrollment.SUBMITTED': 3,
        'Participant.consentForStudyEnrollmentAndEHR.SUBMITTED': 1,
        'Participant.consentForStudyEnrollmentAndEHR.UNSET': 2,
        'Participant.enrollmentStatus.FULL_PARTICIPANT': 1,
        'Participant.enrollmentStatus.INTERESTED': 2,
        'Participant.genderIdentity.PMI_PreferNotToAnswer': 1,
        'Participant.genderIdentity.PMI_Skip': 1,
        'Participant.genderIdentity.female': 1,
        'Participant.hpoId.AZ_TUCSON': 1,
        'Participant.hpoId.PITT': 2,
        'Participant.numCompletedBaselinePPIModules.1': 2,
        'Participant.numCompletedBaselinePPIModules.3': 1,
        'Participant.physicalMeasurements.COMPLETED': 1,
        'Participant.physicalMeasurements.UNSET': 2,
        'Participant.questionnaireOnFamilyHealth.UNSET': 3,
        'Participant.questionnaireOnHealthcareAccess.UNSET': 3,
        'Participant.questionnaireOnLifestyle.SUBMITTED': 1,
        'Participant.questionnaireOnLifestyle.UNSET': 2,
        'Participant.questionnaireOnMedicalHistory.UNSET': 3,
        'Participant.questionnaireOnMedications.UNSET': 3,
        'Participant.questionnaireOnOverallHealth.SUBMITTED': 1,
        'Participant.questionnaireOnOverallHealth.UNSET': 2,
        'Participant.questionnaireOnTheBasics.SUBMITTED': 3,
        'Participant.race.OTHER_RACE': 1,
        'Participant.race.PMI_Skip': 1,
        'Participant.race.UNSET': 1,
        'Participant.samplesToIsolateDNA.RECEIVED': 1,
        'Participant.samplesToIsolateDNA.UNSET': 2,
        'Participant.state.PIIState_VA': 1,
        'Participant.state.PMI_Skip': 1,
        'Participant.state.UNSET': 1,

        'FullParticipant.ageRange.UNSET': 1,
        'FullParticipant.biospecimen.UNSET': 1,
        'FullParticipant.biospecimenSamples.SAMPLES_ARRIVED': 1,
        'FullParticipant.biospecimenSummary.SAMPLES_ARRIVED': 1,
        'FullParticipant.censusRegion.SOUTH': 1,
        'FullParticipant.consentForElectronicHealthRecords.SUBMITTED': 1,
        'FullParticipant.consentForStudyEnrollment.SUBMITTED': 1,
        'FullParticipant.consentForStudyEnrollmentAndEHR.SUBMITTED': 1,
        'FullParticipant.enrollmentStatus.FULL_PARTICIPANT': 1,
        'FullParticipant.genderIdentity.PMI_PreferNotToAnswer': 1,
        'FullParticipant.hpoId.PITT': 1,
        'FullParticipant.numCompletedBaselinePPIModules.3': 1,
        'FullParticipant.physicalMeasurements.COMPLETED': 1,
        'FullParticipant.questionnaireOnFamilyHealth.UNSET': 1,
        'FullParticipant.questionnaireOnHealthcareAccess.UNSET': 1,
        'FullParticipant.questionnaireOnLifestyle.SUBMITTED': 1,
        'FullParticipant.questionnaireOnMedicalHistory.UNSET': 1,
        'FullParticipant.questionnaireOnMedications.UNSET': 1,
        'FullParticipant.questionnaireOnOverallHealth.SUBMITTED': 1,
        'FullParticipant.questionnaireOnTheBasics.SUBMITTED': 1,
        'FullParticipant.race.OTHER_RACE': 1,
        'FullParticipant.samplesToIsolateDNA.RECEIVED': 1,
        'FullParticipant.state.PIIState_VA': 1,
    })

    # There is a biobank order on 1/4, but it gets ignored since it's after the run date.
    self.assertBucket(bucket_map, TIME_4, '')

  def assertBucket(self, bucket_map, dt, hpoId, metrics=None):
    bucket = bucket_map.get((dt.date(), hpoId))
    if metrics:
      self.assertIsNotNone(bucket)
      self.assertMultiLineEqual(
          pretty(metrics), pretty(json.loads(bucket.metrics)))
    else:
      self.assertIsNone(bucket)
