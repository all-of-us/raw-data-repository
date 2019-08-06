import datetime
import httplib
import urllib

from rdr_service.clock import FakeClock
from rdr_service.code_constants import (PMI_SKIP_CODE, PPI_SYSTEM, RACE_AIAN_CODE,
                                        RACE_HISPANIC_CODE, RACE_MENA_CODE,
                                        RACE_NONE_OF_THESE_CODE, RACE_WHITE_CODE)
from rdr_service.concepts import Concept
from rdr_service.dao.calendar_dao import CalendarDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metrics_cache_dao import MetricsAgeCacheDao, MetricsEnrollmentStatusCacheDao, \
  MetricsGenderCacheDao, MetricsLanguageCacheDao, MetricsLifecycleCacheDao, MetricsRaceCacheDao, \
  MetricsRegionCacheDao
from rdr_service.dao.participant_counts_over_time_service import ParticipantCountsOverTimeService
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.calendar import Calendar
from rdr_service.model.code import Code, CodeType
from rdr_service.model.hpo import HPO
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import EnrollmentStatus, MetricsAPIVersion, MetricsCacheType, \
  OrganizationType, \
  TEST_HPO_ID, TEST_HPO_NAME, WithdrawalStatus, make_primary_provider_link_for_name
from rdr_service.test.unit_test.unit_test_util import FlaskTestBase, \
  make_questionnaire_response_json

TIME_1 = datetime.datetime(2017, 12, 31)

def _questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id

class ParticipantCountsOverTimeApiTest(FlaskTestBase):

  provider_link = {
    "primary": True,
    "organization": {
      "display": None,
      "reference": "Organization/PITT",
    }
  }

  az_provider_link = {
    "primary": True,
    "organization": {
      "display": None,
      "reference": "Organization/AZ_TUCSON",
    }
  }

  code_link_ids = (
    'race', 'genderIdentity', 'state', 'sex', 'sexualOrientation', 'recontactMethod', 'language',
    'education', 'income'
  )

  string_link_ids = (
    'firstName', 'middleName', 'lastName', 'streetAddress', 'city', 'phoneNumber', 'zipCode'
  )

  def setUp(self):
    super(ParticipantCountsOverTimeApiTest, self).setUp(use_mysql=True)
    self.dao = ParticipantDao()
    self.ps_dao = ParticipantSummaryDao()
    self.ps = ParticipantSummary()
    self.calendar_dao = CalendarDao()
    self.hpo_dao = HPODao()
    self.code_dao = CodeDao()

    # Needed by ParticipantCountsOverTimeApi
    self.hpo_dao.insert(HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName='Test',
                       organizationType=OrganizationType.UNSET))

    self.time0 = datetime.datetime(2017, 10, 3)
    self.time1 = datetime.datetime(2017, 12, 31)
    self.time2 = datetime.datetime(2018, 1, 1)
    self.time3 = datetime.datetime(2018, 1, 2)
    self.time4 = datetime.datetime(2018, 1, 3)
    self.time5 = datetime.datetime(2018, 1, 4)

    # Insert 2 weeks of dates
    curr_date = datetime.date(2017, 12, 22)
    for _ in xrange(0, 18):
      calendar_day = Calendar(day=curr_date )
      CalendarDao().insert(calendar_day)
      curr_date = curr_date + datetime.timedelta(days=1)

  def _insert(self, participant, first_name=None, last_name=None, hpo_name=None,
              unconsented=False, time_int=None, time_study=None, time_mem=None, time_fp=None,
              time_fp_stored=None, gender_id=None, dob=None, state_id=None, primary_language=None,
              gender_identity=None, **ppi_modules):
    """
    Create a participant in a transient test database.

    :param participant: Participant object
    :param first_name: First name
    :param last_name: Last name
    :param hpo_name: HPO name (one of PITT or AZ_TUCSON)
    :param time_int: Time that participant fulfilled INTERESTED criteria
    :param time_mem: Time that participant fulfilled MEMBER criteria
    :param time_fp: Time that participant fulfilled FULL_PARTICIPANT criteria
    :return: Participant object
    """

    if unconsented is True:
      enrollment_status = None
    elif time_mem is None:
      enrollment_status = EnrollmentStatus.INTERESTED
    elif time_fp is None:
      enrollment_status = EnrollmentStatus.MEMBER
    else:
      enrollment_status = EnrollmentStatus.FULL_PARTICIPANT

    with FakeClock(time_int):
      self.dao.insert(participant)

    participant.providerLink = make_primary_provider_link_for_name(hpo_name)
    with FakeClock(time_mem):
      self.dao.update(participant)

    if enrollment_status is None:
      return None

    summary = self.participant_summary(participant)

    if first_name:
      summary.firstName = first_name
    if last_name:
      summary.lastName = last_name

    if gender_id:
      summary.genderIdentityId = gender_id
    if gender_identity:
      summary.genderIdentity = gender_identity
    if dob:
      summary.dateOfBirth = dob
    else:
      summary.dateOfBirth = datetime.date(1978, 10, 10)
    if state_id:
      summary.stateId = state_id

    if primary_language:
      summary.primaryLanguage = primary_language

    summary.enrollmentStatus = enrollment_status

    summary.enrollmentStatusMemberTime = time_mem
    summary.enrollmentStatusCoreOrderedSampleTime = time_fp
    summary.enrollmentStatusCoreStoredSampleTime = time_fp_stored

    summary.hpoId = self.hpo_dao.get_by_name(hpo_name).hpoId

    if time_study is not None:
      with FakeClock(time_mem):
        summary.consentForStudyEnrollmentTime = time_study

    if time_mem is not None:
      with FakeClock(time_mem):
        summary.consentForElectronicHealthRecords = 1
        summary.consentForElectronicHealthRecordsTime = time_mem

    if time_fp is not None:
      with FakeClock(time_fp):
        if not summary.consentForElectronicHealthRecords:
          summary.consentForElectronicHealthRecords = 1
          summary.consentForElectronicHealthRecordsTime = time_fp
        summary.questionnaireOnTheBasicsTime = time_fp
        summary.questionnaireOnLifestyleTime = time_fp
        summary.questionnaireOnOverallHealthTime = time_fp
        summary.questionnaireOnHealthcareAccessTime = time_fp
        summary.questionnaireOnMedicalHistoryTime = time_fp
        summary.questionnaireOnMedicationsTime = time_fp
        summary.questionnaireOnFamilyHealthTime = time_fp
        summary.physicalMeasurementsFinalizedTime = time_fp
        summary.physicalMeasurementsTime = time_fp
        summary.sampleOrderStatus1ED04Time = time_fp
        summary.sampleOrderStatus1SALTime = time_fp
        summary.sampleStatus1ED04Time = time_fp
        summary.sampleStatus1SALTime = time_fp

    if ppi_modules:
      summary.questionnaireOnTheBasicsTime = ppi_modules['questionnaireOnTheBasicsTime']
      summary.questionnaireOnLifestyleTime = ppi_modules['questionnaireOnLifestyleTime']
      summary.questionnaireOnOverallHealthTime = ppi_modules['questionnaireOnOverallHealthTime']
      summary.questionnaireOnHealthcareAccessTime = ppi_modules['questionnaireOnHealthcareAccessTime']
      summary.questionnaireOnMedicalHistoryTime = ppi_modules['questionnaireOnMedicalHistoryTime']
      summary.questionnaireOnMedicationsTime = ppi_modules['questionnaireOnMedicationsTime']
      summary.questionnaireOnFamilyHealthTime = ppi_modules['questionnaireOnFamilyHealthTime']

    self.ps_dao.insert(summary)

    return summary

  def update_participant_summary(self, participant_id, time_mem=None, time_fp=None,
                                 time_fp_stored=None, time_study=None):

    participant = self.dao.get(participant_id)
    summary = self.participant_summary(participant)
    if time_mem is None:
      enrollment_status = EnrollmentStatus.INTERESTED
    elif time_fp is None:
      enrollment_status = EnrollmentStatus.MEMBER
    else:
      enrollment_status = EnrollmentStatus.FULL_PARTICIPANT

    summary.enrollmentStatus = enrollment_status

    summary.enrollmentStatusMemberTime = time_mem
    summary.enrollmentStatusCoreOrderedSampleTime = time_fp
    summary.enrollmentStatusCoreStoredSampleTime = time_fp_stored

    if time_study is not None:
      with FakeClock(time_mem):
        summary.consentForStudyEnrollmentTime = time_study

    if time_mem is not None:
      with FakeClock(time_mem):
        summary.consentForElectronicHealthRecords = 1
        summary.consentForElectronicHealthRecordsTime = time_mem

    if time_fp is not None:
      with FakeClock(time_fp):
        if not summary.consentForElectronicHealthRecords:
          summary.consentForElectronicHealthRecords = 1
          summary.consentForElectronicHealthRecordsTime = time_fp
        summary.questionnaireOnTheBasicsTime = time_fp
        summary.questionnaireOnLifestyleTime = time_fp
        summary.questionnaireOnOverallHealthTime = time_fp
        summary.questionnaireOnHealthcareAccessTime = time_fp
        summary.questionnaireOnMedicalHistoryTime = time_fp
        summary.questionnaireOnMedicationsTime = time_fp
        summary.questionnaireOnFamilyHealthTime = time_fp
        summary.physicalMeasurementsFinalizedTime = time_fp
        summary.physicalMeasurementsTime = time_fp
        summary.sampleOrderStatus1ED04Time = time_fp
        summary.sampleOrderStatus1SALTime = time_fp
        summary.sampleStatus1ED04Time = time_fp
        summary.sampleStatus1SALTime = time_fp

    self.ps_dao.update(summary)

    return summary

  def test_get_counts_with_default_parameters(self):
    # The most basic test in this class

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    # TODO: remove bucketSize from these parameters in all tests
    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    # You can debug API responses easily by uncommenting the lines below
    # print('response')
    # print(response)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 1)

  def test_get_counts_with_single_awardee_filter(self):
    # Does the awardee filter work?

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p1 = Participant(participantId=2, biobankId=5)
    self._insert(p1, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p1 = Participant(participantId=3, biobankId=6)
    self._insert(p1, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)

    # enrollmentStatus param left blank to test we can handle it
    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      &awardee=PITT
      &enrollmentStatus=
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 1)

    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      &awardee=AZ_TUCSON
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 2)

  def test_get_counts_with_single_awardee_filter(self):
    # Does the awardee filter work when passed a single awardee?

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p1 = Participant(participantId=2, biobankId=5)
    self._insert(p1, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p1 = Participant(participantId=3, biobankId=6)
    self._insert(p1, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)

    qs = """
        bucketSize=1
        &stratification=ENROLLMENT_STATUS
        &startDate=2017-12-30
        &endDate=2018-01-04
        &awardee=PITT
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 1)

    qs = """
        bucketSize=1
        &stratification=ENROLLMENT_STATUS
        &startDate=2017-12-30
        &endDate=2018-01-04
        &awardee=AZ_TUCSON
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 2)

  def test_get_counts_with_multiple_awardee_filters(self):
    # Does the awardee filter work when passed more than one awardee?

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)

    qs = """
        bucketSize=1
        &stratification=ENROLLMENT_STATUS
        &startDate=2017-12-30
        &endDate=2018-01-04
        &awardee=PITT,AZ_TUCSON
        &enrollmentStatus=
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 3)

  def test_get_counts_with_enrollment_status_member_filter(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time2)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1, time_mem=self.time3)

    # awardee param intentionally left blank to test we can handle it
    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      &awardee=
      &enrollmentStatus=MEMBER
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    member_count_day_1 = response[0]['metrics']['MEMBER']
    member_count_day_2 = response[1]['metrics']['MEMBER']
    member_count_day_3 = response[2]['metrics']['MEMBER']
    member_count_day_4 = response[3]['metrics']['MEMBER']
    interested_count_day_4 = response[1]['metrics']['INTERESTED']

    self.assertEquals(member_count_day_1, 0)
    self.assertEquals(member_count_day_2, 0)
    self.assertEquals(member_count_day_3, 2)
    self.assertEquals(member_count_day_4, 3)
    self.assertEquals(interested_count_day_4, 0)

    qs = """
      bucketSize=1
      &stratification=TOTAL
      &startDate=2017-12-30
      &endDate=2018-01-04
      &enrollmentStatus=MEMBER
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['TOTAL']
    total_count_day_2 = response[1]['metrics']['TOTAL']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 3)

    # test filter by sample stored time doesn't affect MEMBER and TOTAL
    qs = """
          bucketSize=1
          &stratification=ENROLLMENT_STATUS
          &startDate=2017-12-30
          &endDate=2018-01-04
          &awardee=
          &enrollmentStatus=MEMBER
          &filterBy=STORED
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    member_count_day_1 = response[0]['metrics']['MEMBER']
    member_count_day_2 = response[1]['metrics']['MEMBER']
    member_count_day_3 = response[2]['metrics']['MEMBER']
    member_count_day_4 = response[3]['metrics']['MEMBER']
    interested_count_day_4 = response[1]['metrics']['INTERESTED']

    self.assertEquals(member_count_day_1, 0)
    self.assertEquals(member_count_day_2, 0)
    self.assertEquals(member_count_day_3, 2)
    self.assertEquals(member_count_day_4, 3)
    self.assertEquals(interested_count_day_4, 0)

    qs = """
          bucketSize=1
          &stratification=TOTAL
          &startDate=2017-12-30
          &endDate=2018-01-04
          &enrollmentStatus=MEMBER
          &filterBy=STORED
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['TOTAL']
    total_count_day_2 = response[1]['metrics']['TOTAL']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 3)

  def test_get_counts_with_enrollment_status_full_participant_filter(self):

    # MEMBER @ time 1
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1,
                 time_mem=self.time1)

    # FULL PARTICIPANT @ time 2
    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1,
                 time_mem=self.time1, time_fp=self.time2, time_fp_stored=self.time2)

    # FULL PARTICIPANT @ time 2
    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1,
                 time_mem=self.time1, time_fp=self.time2, time_fp_stored=self.time3)

    # FULL PARTICIPANT @ time 3
    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1,
                 time_mem=self.time1, time_fp=self.time3, time_fp_stored=self.time5)

    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      &enrollmentStatus=FULL_PARTICIPANT
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    full_participant_count_day_1 = response[0]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_2 = response[1]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_3 = response[2]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_4 = response[3]['metrics']['FULL_PARTICIPANT']
    member_count_day_4 = response[4]['metrics']['MEMBER']

    self.assertEquals(full_participant_count_day_1, 0)
    self.assertEquals(full_participant_count_day_2, 0)
    self.assertEquals(full_participant_count_day_3, 2)
    self.assertEquals(full_participant_count_day_4, 3)
    self.assertEquals(member_count_day_4, 0)  # Excluded per enrollmentStatus parameter

    # test filter by sample stored time
    qs = """
          bucketSize=1
          &stratification=ENROLLMENT_STATUS
          &startDate=2017-12-30
          &endDate=2018-01-05
          &enrollmentStatus=FULL_PARTICIPANT
          &filterBy=STORED
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    full_participant_count_day_1 = response[0]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_2 = response[1]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_3 = response[2]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_4 = response[3]['metrics']['FULL_PARTICIPANT']
    full_participant_count_day_6 = response[5]['metrics']['FULL_PARTICIPANT']
    member_count_day_4 = response[4]['metrics']['MEMBER']

    self.assertEquals(full_participant_count_day_1, 0)
    self.assertEquals(full_participant_count_day_2, 0)
    self.assertEquals(full_participant_count_day_3, 1)
    self.assertEquals(full_participant_count_day_4, 2)
    self.assertEquals(full_participant_count_day_6, 3)
    self.assertEquals(member_count_day_4, 0)  # Excluded per enrollmentStatus parameter

  def test_get_counts_with_enrollment_status_v2(self):
    # REGISTERED @ time 1
    p0 = Participant(participantId=5, biobankId=8)
    self._insert(p0, 'Alice2', 'Aardvark2', 'PITT', time_int=self.time1)

    # PARTICIPANT @ time 1
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1,
                 time_study=self.time1)

    # MEMBER @ time 2
    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2)

    # FULL PARTICIPANT @ time 2
    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time2, time_fp_stored=self.time3)

    # FULL PARTICIPANT @ time 3
    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time3, time_fp_stored=self.time5)

    qs = """
          bucketSize=1
          &stratification=ENROLLMENT_STATUS
          &startDate=2017-12-30
          &endDate=2018-01-04
          &version=2
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    registered_count_day_1 = response[0]['metrics']['REGISTERED']
    registered_count_day_2 = response[1]['metrics']['REGISTERED']
    registered_count_day_3 = response[2]['metrics']['REGISTERED']
    registered_count_day_4 = response[3]['metrics']['REGISTERED']

    participant_count_day_1 = response[0]['metrics']['PARTICIPANT']
    participant_count_day_2 = response[1]['metrics']['PARTICIPANT']
    participant_count_day_3 = response[2]['metrics']['PARTICIPANT']
    participant_count_day_4 = response[3]['metrics']['PARTICIPANT']

    consented_count_day_1 = response[0]['metrics']['FULLY_CONSENTED']
    consented_count_day_2 = response[1]['metrics']['FULLY_CONSENTED']
    consented_count_day_3 = response[2]['metrics']['FULLY_CONSENTED']
    consented_count_day_4 = response[3]['metrics']['FULLY_CONSENTED']

    core_count_day_1 = response[0]['metrics']['CORE_PARTICIPANT']
    core_count_day_2 = response[1]['metrics']['CORE_PARTICIPANT']
    core_count_day_3 = response[2]['metrics']['CORE_PARTICIPANT']
    core_count_day_4 = response[3]['metrics']['CORE_PARTICIPANT']

    self.assertEquals(registered_count_day_1, 0)
    self.assertEquals(registered_count_day_2, 1)
    self.assertEquals(registered_count_day_3, 1)
    self.assertEquals(registered_count_day_4, 1)

    self.assertEquals(participant_count_day_1, 0)
    self.assertEquals(participant_count_day_2, 2)
    self.assertEquals(participant_count_day_3, 1)
    self.assertEquals(participant_count_day_4, 1)

    self.assertEquals(consented_count_day_1, 0)
    self.assertEquals(consented_count_day_2, 2)
    self.assertEquals(consented_count_day_3, 2)
    self.assertEquals(consented_count_day_4, 1)

    self.assertEquals(core_count_day_1, 0)
    self.assertEquals(core_count_day_2, 0)
    self.assertEquals(core_count_day_3, 1)
    self.assertEquals(core_count_day_4, 2)

  def test_get_counts_with_enrollment_status_v2_with_enrollment_status_filter(self):
    # REGISTERED @ time 1
    p0 = Participant(participantId=5, biobankId=8)
    self._insert(p0, 'Alice2', 'Aardvark2', 'PITT', time_int=self.time1)

    # PARTICIPANT @ time 1
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1,
                 time_study=self.time1)

    # MEMBER @ time 2
    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2)

    # FULL PARTICIPANT @ time 2
    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time2, time_fp_stored=self.time3)

    # FULL PARTICIPANT @ time 3
    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time3, time_fp_stored=self.time5)

    qs = """
          bucketSize=1
          &stratification=ENROLLMENT_STATUS
          &startDate=2017-12-30
          &endDate=2018-01-04
          &enrollmentStatus=PARTICIPANT,CORE_PARTICIPANT
          &version=2
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    registered_count_day_1 = response[0]['metrics']['REGISTERED']
    registered_count_day_2 = response[1]['metrics']['REGISTERED']
    registered_count_day_3 = response[2]['metrics']['REGISTERED']
    registered_count_day_4 = response[3]['metrics']['REGISTERED']

    participant_count_day_1 = response[0]['metrics']['PARTICIPANT']
    participant_count_day_2 = response[1]['metrics']['PARTICIPANT']
    participant_count_day_3 = response[2]['metrics']['PARTICIPANT']
    participant_count_day_4 = response[3]['metrics']['PARTICIPANT']

    consented_count_day_1 = response[0]['metrics']['FULLY_CONSENTED']
    consented_count_day_2 = response[1]['metrics']['FULLY_CONSENTED']
    consented_count_day_3 = response[2]['metrics']['FULLY_CONSENTED']
    consented_count_day_4 = response[3]['metrics']['FULLY_CONSENTED']

    core_count_day_1 = response[0]['metrics']['CORE_PARTICIPANT']
    core_count_day_2 = response[1]['metrics']['CORE_PARTICIPANT']
    core_count_day_3 = response[2]['metrics']['CORE_PARTICIPANT']
    core_count_day_4 = response[3]['metrics']['CORE_PARTICIPANT']

    self.assertEquals(registered_count_day_1, 0)
    self.assertEquals(registered_count_day_2, 0)
    self.assertEquals(registered_count_day_3, 0)
    self.assertEquals(registered_count_day_4, 0)

    self.assertEquals(participant_count_day_1, 0)
    self.assertEquals(participant_count_day_2, 2)
    self.assertEquals(participant_count_day_3, 1)
    self.assertEquals(participant_count_day_4, 1)

    self.assertEquals(consented_count_day_1, 0)
    self.assertEquals(consented_count_day_2, 0)
    self.assertEquals(consented_count_day_3, 0)
    self.assertEquals(consented_count_day_4, 0)

    self.assertEquals(core_count_day_1, 0)
    self.assertEquals(core_count_day_2, 0)
    self.assertEquals(core_count_day_3, 1)
    self.assertEquals(core_count_day_4, 2)

  def test_get_counts_with_enrollment_status_v2_with_awardee_filter(self):
    # REGISTERED @ time 1
    p0 = Participant(participantId=5, biobankId=8)
    self._insert(p0, 'Alice2', 'Aardvark2', 'PITT', time_int=self.time1)

    # PARTICIPANT @ time 1
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1,
                 time_study=self.time1)

    # MEMBER @ time 2
    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2)

    # FULL PARTICIPANT @ time 2
    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time2, time_fp_stored=self.time3)

    # FULL PARTICIPANT @ time 3
    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time3, time_fp_stored=self.time5)

    qs = """
          bucketSize=1
          &stratification=ENROLLMENT_STATUS
          &startDate=2017-12-30
          &endDate=2018-01-04
          &awardee=PITT
          &version=2
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    registered_count_day_1 = response[0]['metrics']['REGISTERED']
    registered_count_day_2 = response[1]['metrics']['REGISTERED']
    registered_count_day_3 = response[2]['metrics']['REGISTERED']
    registered_count_day_4 = response[3]['metrics']['REGISTERED']

    participant_count_day_1 = response[0]['metrics']['PARTICIPANT']
    participant_count_day_2 = response[1]['metrics']['PARTICIPANT']
    participant_count_day_3 = response[2]['metrics']['PARTICIPANT']
    participant_count_day_4 = response[3]['metrics']['PARTICIPANT']

    consented_count_day_1 = response[0]['metrics']['FULLY_CONSENTED']
    consented_count_day_2 = response[1]['metrics']['FULLY_CONSENTED']
    consented_count_day_3 = response[2]['metrics']['FULLY_CONSENTED']
    consented_count_day_4 = response[3]['metrics']['FULLY_CONSENTED']

    core_count_day_1 = response[0]['metrics']['CORE_PARTICIPANT']
    core_count_day_2 = response[1]['metrics']['CORE_PARTICIPANT']
    core_count_day_3 = response[2]['metrics']['CORE_PARTICIPANT']
    core_count_day_4 = response[3]['metrics']['CORE_PARTICIPANT']

    self.assertEquals(registered_count_day_1, 0)
    self.assertEquals(registered_count_day_2, 1)
    self.assertEquals(registered_count_day_3, 1)
    self.assertEquals(registered_count_day_4, 1)

    self.assertEquals(participant_count_day_1, 0)
    self.assertEquals(participant_count_day_2, 1)
    self.assertEquals(participant_count_day_3, 1)
    self.assertEquals(participant_count_day_4, 1)

    self.assertEquals(consented_count_day_1, 0)
    self.assertEquals(consented_count_day_2, 1)
    self.assertEquals(consented_count_day_3, 1)
    self.assertEquals(consented_count_day_4, 0)

    self.assertEquals(core_count_day_1, 0)
    self.assertEquals(core_count_day_2, 0)
    self.assertEquals(core_count_day_3, 0)
    self.assertEquals(core_count_day_4, 1)

  def test_get_counts_with_total_enrollment_status_full_participant_filter(self):
    # When filtering with TOTAL stratification, filtered participants are
    # returned by their sign up date, not the date they reached their highest
    # enrollment status.

    # MEMBER @ time 1
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1,
                 time_mem=self.time1)

    # FULL PARTICIPANT @ time 2
    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1,
                 time_mem=self.time1, time_fp=self.time2)

    # FULL PARTICIPANT @ time 2
    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1,
                 time_mem=self.time1, time_fp=self.time2)

    # FULL PARTICIPANT @ time 3
    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1,
                 time_mem=self.time1, time_fp=self.time3)

    qs = """
      bucketSize=1
      &stratification=TOTAL
      &startDate=2017-12-30
      &endDate=2018-01-04
      &enrollmentStatus=FULL_PARTICIPANT
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['TOTAL']
    total_count_day_2 = response[1]['metrics']['TOTAL']
    total_count_day_3 = response[2]['metrics']['TOTAL']
    total_count_day_4 = response[3]['metrics']['TOTAL']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 3)
    self.assertEquals(total_count_day_3, 3)
    self.assertEquals(total_count_day_4, 3)

  def test_get_counts_with_single_various_filters(self):
    # Do the awardee and enrollment status filters work when passed single values?

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1,
                 time_mem=self.time1)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', time_int=self.time1,
                 time_mem=self.time1)

    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      &awardee=PITT
      &enrollmentStatus=MEMBER
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']
    member_count_day_2 = response[1]['metrics']['MEMBER']

    self.assertEquals(interested_count_day_1, 0)

    # We requested data for only MEMBERs, so no INTERESTEDs should be returned
    self.assertEquals(interested_count_day_2, 0)

    # We requested data for only MEMBERs in PITT, so no MEMBERs in AZ_TUCSON should be returned
    self.assertEquals(member_count_day_2, 1)

  def test_get_counts_with_multiple_various_filters(self):
    # Do the awardee and enrollment status filters work when passed multiple values?

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)

    qs = """
        bucketSize=1
        &stratification=ENROLLMENT_STATUS
        &startDate=2017-12-30
        &endDate=2018-01-04
        &awardee=AZ_TUCSON,PITT
        &enrollmentStatus=INTERESTED,MEMBER,FULL_PARTICIPANT
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 3)

  def test_get_counts_with_total_stratification_unfiltered(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)

    qs = """
      bucketSize=1
      &stratification=TOTAL
      &startDate=2017-12-30
      &endDate=2018-01-04
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['TOTAL']
    total_count_day_2 = response[1]['metrics']['TOTAL']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 3)

  def test_get_counts_excluding_interested_participants(self):
    # When filtering only for MEMBER, no INTERESTED (neither consented nor unconsented) should be counted

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time1)

    qs = """
        bucketSize=1
        &stratification=ENROLLMENT_STATUS
        &startDate=2017-12-30
        &endDate=2018-01-04
        &enrollmentStatus=MEMBER
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_2 = response[1]['metrics']['INTERESTED']
    member_count_day_2 = response[1]['metrics']['MEMBER']

    self.assertEquals(interested_count_day_2, 0)
    self.assertEquals(member_count_day_2, 1)

  def test_get_counts_excluding_withdrawn_participants(self):
    # Withdrawn participants should not appear in counts

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    ps3 = self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)
    ps3.withdrawalStatus = WithdrawalStatus.NO_USE  # Chad withdrew from the study
    self.ps_dao.update(ps3)

    qs = """
        bucketSize=1
        &stratification=TOTAL
        &startDate=2017-12-30
        &endDate=2018-01-04
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['TOTAL']
    total_count_day_2 = response[1]['metrics']['TOTAL']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 2)

  def test_get_counts_for_unconsented_individuals(self):
    # Those who have signed up but not consented should be INTERESTED

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1)

    qs = """
          bucketSize=1
          &stratification=ENROLLMENT_STATUS
          &startDate=2017-12-30
          &endDate=2018-01-04
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['INTERESTED']
    total_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 3)

  def test_url_parameter_validation_for_date_range(self):
    # Ensure requests for very long date ranges are marked BAD REQUEST

    qs = """
        bucketSize=1
        &stratification=TOTAL
        &startDate=2017-12-30
        &endDate=2217-12-30
        """
    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs,
                             expected_status=httplib.BAD_REQUEST)
    self.assertEquals(response, None)

  def test_url_parameter_validation_for_stratifications(self):
    # Ensure requests invalid stratifications are marked BAD REQUEST

    qs = """
          bucketSize=1
          &stratification=FOOBAR
          &startDate=2017-12-30
          &endDate=2018-01-04
          """
    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs,
                             expected_status=httplib.BAD_REQUEST)
    self.assertEquals(response, None)

  def test_url_parameter_validation_for_awardee(self):
    # Ensure requests invalid awardee are marked BAD REQUEST

    qs = """
            bucketSize=1
            &stratification=ENROLLMENT_STATUS
            &startDate=2017-12-30
            &endDate=2018-01-04
            &awardee=FOOBAR
            """
    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs,
                             expected_status=httplib.BAD_REQUEST)
    self.assertEquals(response, None)

  def test_url_parameter_validation_for_enrollment_status(self):
    # Ensure requests invalid enrollment status are marked BAD REQUEST

    qs = """
            bucketSize=1
            &stratification=ENROLLMENT_STATUS
            &startDate=2017-12-30
            &endDate=2018-01-04
            &enrollmentStatus=FOOBAR
            """
    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs,
                             expected_status=httplib.BAD_REQUEST)
    self.assertEquals(response, None)

  # Add tests for more invalida parameters, e.g.:
  # * starting or ending halfway through the data
  # * startDate = endDate
  # * missing required parameters

  def test_refresh_metrics_enrollment_status_cache_data(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time3, time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2018-01-01', '2018-01-08')

    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 2L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 1L, 'core': 0L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 0L, 'core': 1L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 0L, 'core': 1L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-07', 'metrics': {'consented': 0L, 'core': 1L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-06', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, results)

  def test_refresh_metrics_enrollment_status_cache_data_v2(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time1,
                 time_study=self.time1, time_mem=self.time3, time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao(version=MetricsAPIVersion.V2)
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2018-01-01', '2018-01-08')

    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L,
                                                     'participant': 2L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 2L, 'core': 0L, 'registered': 1L,
                                                     'participant': 0L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 1L, 'core': 1L, 'registered': 1L,
                                                     'participant': 0L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 1L, 'core': 1L, 'registered': 1L,
                                                     'participant': 0L},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L,
                                                     'participant': 0L},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L,
                                                     'participant': 0L},
                   'hpo': u'UNSET'}, results)

  def test_refresh_metrics_enrollment_status_cache_data_for_public_metrics_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time3, time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2018-01-01', '2018-01-08')
    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0, 'core': 0, 'registered': 3}},
                  results)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 1, 'core': 0, 'registered': 2}},
                  results)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 0, 'core': 1, 'registered': 2}},
                  results)

  def test_get_history_enrollment_status_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time3, time_fp_stored=self.time4)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=4, biobankId=7, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=ENROLLMENT_STATUS
          &startDate=2018-01-01
          &endDate=2018-01-08
          &history=TRUE
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 2L},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 1L, 'core': 0L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 0L, 'core': 1L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 0L, 'core': 1L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-07', 'metrics': {'consented': 0L, 'core': 1L, 'registered': 1L},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, response)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, response)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, response)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, response)
    self.assertIn({'date': '2018-01-06', 'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L},
                   'hpo': u'UNSET'}, response)

  def test_get_history_enrollment_status_api_v2(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time3, time_fp_stored=self.time4)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=4, biobankId=7, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=ENROLLMENT_STATUS
          &startDate=2018-01-01
          &endDate=2018-01-08
          &history=TRUE
          &version=2
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({u'date': u'2018-01-01', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 1},
                   u'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({u'date': u'2018-01-02', u'metrics': {u'consented': 1, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({u'date': u'2018-01-03', u'metrics': {u'consented': 0, u'core': 1,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({u'date': u'2018-01-04', u'metrics': {u'consented': 0, u'core': 1,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({u'date': u'2018-01-05', u'metrics': {u'consented': 0, u'core': 1,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)

    self.assertIn({u'date': u'2018-01-01', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'UNSET'}, response)
    self.assertIn({u'date': u'2018-01-02', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'UNSET'}, response)
    self.assertIn({u'date': u'2018-01-03', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'UNSET'}, response)
    self.assertIn({u'date': u'2018-01-04', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'UNSET'}, response)
    self.assertIn({u'date': u'2018-01-05', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'UNSET'}, response)

  def test_get_history_enrollment_status_api_filtered_by_awardee(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=4, biobankId=7, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=ENROLLMENT_STATUS
          &startDate=2018-01-01
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertNotIn({'date': '2018-01-01',
                      'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L}, 'hpo': u'UNSET'},
                     response)
    self.assertNotIn({'date': '2018-01-06',
                      'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L}, 'hpo': u'UNSET'},
                     response)
    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0, 'core': 0, 'registered': 0},
                   'hpo': 'PITT'}, response)
    self.assertIn({'date': '2018-01-03', 'metrics': {'consented': 1, 'core': 0, 'registered': 0},
                   'hpo': 'PITT'}, response)
    self.assertIn({'date': '2018-01-04', 'metrics': {'consented': 0, 'core': 1, 'registered': 0},
                   'hpo': 'PITT'}, response)
    self.assertIn({'date': '2018-01-01', 'metrics': {'consented': 0, 'core': 0, 'registered': 1},
                   'hpo': 'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-02', 'metrics': {'consented': 0, 'core': 0, 'registered': 1},
                   'hpo': 'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-08', 'metrics': {'consented': 0, 'core': 0, 'registered': 1},
                   'hpo': 'AZ_TUCSON'}, response)

  def test_get_history_enrollment_status_api_filtered_by_awardee_v2(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=4, biobankId=7, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=ENROLLMENT_STATUS
          &startDate=2018-01-01
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          &version=2
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertNotIn({'date': '2018-01-01',
                      'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L, 'participant': 0L},
                      'hpo': u'UNSET'}, response)
    self.assertNotIn({'date': '2018-01-06',
                      'metrics': {'consented': 0L, 'core': 0L, 'registered': 1L, 'participant': 0L},
                      'hpo': u'UNSET'}, response)
    self.assertIn({u'date': u'2018-01-02', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 0, u'participant': 1},
                   u'hpo': u'PITT'}, response)
    self.assertIn({u'date': u'2018-01-03', u'metrics': {u'consented': 1, u'core': 0,
                                                        u'registered': 0, u'participant': 0},
                   u'hpo': u'PITT'}, response)
    self.assertIn({u'date': u'2018-01-04', u'metrics': {u'consented': 0, u'core': 1,
                                                        u'registered': 0, u'participant': 0},
                   u'hpo': u'PITT'}, response)
    self.assertIn({u'date': u'2018-01-01', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({u'date': u'2018-01-03', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({u'date': u'2018-01-07', u'metrics': {u'consented': 0, u'core': 0,
                                                        u'registered': 1, u'participant': 0},
                   u'hpo': u'AZ_TUCSON'}, response)

  def test_refresh_metrics_gender_cache_data(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_identity=3)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_identity=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_identity=5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, gender_identity=5)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'AZ_TUCSON', time_int=self.time5, gender_identity=7)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_identity=5)

    service = ParticipantCountsOverTimeService()
    dao = MetricsGenderCacheDao()
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')

    self.assertIn({'date': '2017-12-31',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 0, 'More than one gender identity': 0},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 0, 'More than one gender identity': 0},
                   'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L, 'More than one gender identity': 0},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L, 'More than one gender identity': 0},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-04',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L, 'More than one gender identity': 1},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L, 'More than one gender identity': 1},
                   'hpo': u'AZ_TUCSON'}, results)

  def test_refresh_metrics_gender_cache_data_for_public_metrics_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_identity=3)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_identity=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_identity=5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, gender_identity=5)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'AZ_TUCSON', time_int=self.time5, gender_identity=7)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_identity=5)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsGenderCacheDao(MetricsCacheType.METRICS_V2_API))
    service.refresh_data_for_metrics_cache(MetricsGenderCacheDao(
      MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
    dao = MetricsGenderCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)

    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')
    self.assertIn({'date': '2017-12-31',
                   'metrics': {u'Woman': 1, u'PMI_Skip': 0, u'Other/Additional Options': 0,
                               u'Non-Binary': 0, 'UNMAPPED': 0, u'Transgender': 0,
                               u'Prefer not to say': 0, u'UNSET': 0, u'Man': 0,
                               'More than one gender identity': 0}}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {u'Woman': 1, u'PMI_Skip': 0, u'Other/Additional Options': 0,
                               u'Non-Binary': 0, 'UNMAPPED': 0, u'Transgender': 0,
                               u'Prefer not to say': 0, u'UNSET': 0, u'Man': 1,
                               'More than one gender identity': 0}}, results)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {u'Woman': 1, u'PMI_Skip': 0, u'Other/Additional Options': 0,
                               u'Non-Binary': 0, 'UNMAPPED': 0, u'Transgender': 1,
                               u'Prefer not to say': 0, u'UNSET': 0, u'Man': 1,
                               'More than one gender identity': 0}}, results)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {u'Woman': 1, u'PMI_Skip': 0, u'Other/Additional Options': 0,
                               u'Non-Binary': 0, 'UNMAPPED': 0, u'Transgender': 2,
                               u'Prefer not to say': 0, u'UNSET': 0, u'Man': 1,
                               'More than one gender identity': 0}}, results)
    self.assertIn({'date': '2018-01-04',
                   'metrics': {u'Woman': 1, u'PMI_Skip': 0, u'Other/Additional Options': 0,
                               u'Non-Binary': 0, 'UNMAPPED': 0, u'Transgender': 2,
                               u'Prefer not to say': 0, u'UNSET': 0, u'Man': 1,
                               'More than one gender identity': 1}}, results)

  def test_get_history_gender_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_identity=3)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_identity=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_identity=5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, gender_identity=5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_identity=5)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsGenderCacheDao())

    qs = """
          &stratification=GENDER_IDENTITY
          &startDate=2017-12-31
          &endDate=2018-01-08
          &history=TRUE
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2017-12-31',
                   'metrics': {'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': u'UNSET'},
                  response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': u'UNSET'},
                  response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)

  def test_get_history_gender_api_filtered_by_awardee(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_identity=3)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_identity=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_identity=5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time4, gender_identity=5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_identity=5)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsGenderCacheDao())

    qs = """
          &stratification=GENDER_IDENTITY
          &startDate=2017-12-31
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertNotIn({'date': '2017-12-31',
                      'metrics': {'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                                  'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                                  'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                                  'More than one gender identity': 0}, 'hpo': u'UNSET'},
                     response)
    self.assertNotIn({'date': '2018-01-01',
                      'metrics': {'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                                  'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                                  'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                                  'More than one gender identity': 0}, 'hpo': u'UNSET'},
                     response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 1L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 1L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 1,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': 'PITT'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 1,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': 'PITT'},
                  response)

  def test_get_history_gender_api_filtered_by_awardee_and_enrollment_status(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_identity=3)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_mem=self.time3,
                 gender_identity=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_mem=self.time4,
                 gender_identity=5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time4, time_mem=self.time5,
                 gender_identity=5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_identity=5)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsGenderCacheDao())

    qs = """
          &stratification=GENDER_IDENTITY
          &startDate=2017-12-31
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          &enrollmentStatus=MEMBER
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2018-01-02',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 1L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 1L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L,
                               'More than one gender identity': 0}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': 'PITT'},
                  response)
    self.assertIn({'date': '2018-01-04',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 1,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': 'PITT'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 1,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0,
                               'More than one gender identity': 0}, 'hpo': 'PITT'},
                  response)

  def test_refresh_metrics_age_range_cache_data(self):

    dob1 = datetime.date(1978, 10, 10)
    dob2 = datetime.date(1988, 10, 10)
    dob3 = datetime.date(1988, 10, 10)
    dob4 = datetime.date(1998, 10, 10)
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, dob=dob1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, dob=dob2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, dob=dob3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, dob=dob4)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, dob=dob3)

    service = ParticipantCountsOverTimeService()
    dao = MetricsAgeCacheDao()
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')

    self.assertIn({'date': '2017-12-31',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 1L,
                               '26-35': 0, '66-75': 0, 'UNSET': 0, '56-65': 0}, 'hpo': u'UNSET'},
                  results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 1L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-06',
                   'metrics': {'0-17': 0, '18-25': 1L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'0-17': 0, '18-25': 1L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, results)

    # test public metrics export cache
    dao = MetricsAgeCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')

    self.assertIn({'date': '2017-12-31',
                   'metrics': {u'50-59': 0, u'60-69': 0, u'30-39': 1, u'40-49': 0, u'UNSET': 0,
                               u'80-89': 0, u'90-': 0, u'18-29': 0, u'70-79': 0}}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {u'50-59': 0, u'60-69': 0, u'30-39': 1, u'40-49': 0, u'18-29': 1,
                               u'80-89': 0, u'90-': 0, u'UNSET': 0, u'70-79': 0}}, results)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {u'50-59': 0, u'60-69': 0, u'30-39': 1, u'40-49': 0, u'18-29': 2,
                               u'80-89': 0, u'70-79': 0, u'UNSET': 0, u'90-': 0}}, results)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {u'50-59': 0, u'60-69': 0, u'30-39': 1, u'40-49': 0, u'18-29': 3,
                               u'80-89': 0, u'70-79': 0, u'UNSET': 0, u'90-': 0}}, results)

  def test_get_history_age_range_api(self):

    dob1 = datetime.date(1978, 10, 10)
    dob2 = datetime.date(1988, 10, 10)
    dob3 = datetime.date(1988, 10, 10)
    dob4 = datetime.date(1998, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, dob=dob1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, dob=dob2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, dob=dob3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, dob=dob4)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, dob=dob3)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsAgeCacheDao())

    qs = """
          &stratification=AGE_RANGE
          &startDate=2017-12-31
          &endDate=2018-01-08
          &history=TRUE
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2017-12-31',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 1L,
                               '26-35': 0, '66-75': 0, 'UNSET': 0, '56-65': 0}, 'hpo': u'UNSET'},
                  response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 1L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-06',
                   'metrics': {'0-17': 0, '18-25': 1L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'0-17': 0, '18-25': 1L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)

  def test_get_history_age_range_api_filtered_by_awardee(self):

    dob1 = datetime.date(1978, 10, 10)
    dob2 = datetime.date(1988, 10, 10)
    dob3 = datetime.date(1988, 10, 10)
    dob4 = datetime.date(1998, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, dob=dob1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, dob=dob2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, dob=dob3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time4, dob=dob4)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, dob=dob3)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsAgeCacheDao())

    qs = """
          &stratification=AGE_RANGE
          &startDate=2017-12-31
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertNotIn({'date': '2017-12-31',
                      'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0,
                                  '36-45': 1L, '26-35': 0, '66-75': 0, 'UNSET': 0, '56-65': 0},
                      'hpo': u'UNSET'}, response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 1L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-06',
                   'metrics': {'0-17': 0, '18-25': 0L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'0-17': 0, '18-25': 1L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 0L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'PITT'}, response)

  def test_get_history_age_range_api_filtered_by_awardee_and_enrollment_status(self):

    dob1 = datetime.date(1978, 10, 10)
    dob2 = datetime.date(1988, 10, 10)
    dob3 = datetime.date(1988, 10, 10)
    dob4 = datetime.date(1998, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, dob=dob1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_mem=self.time3,
                 dob=dob2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_mem=self.time4,
                 dob=dob3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time4, time_mem=self.time5,
                 dob=dob4)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, dob=dob3)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsAgeCacheDao())

    qs = """
          &stratification=AGE_RANGE
          &startDate=2017-12-31
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          &enrollmentStatus=MEMBER
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2018-01-01',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 0L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'0-17': 0, '18-25': 0, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 1L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-06',
                   'metrics': {'0-17': 0, '18-25': 0L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 2L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'AZ_TUCSON'}, response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'0-17': 0, '18-25': 1L, '46-55': 0, '86-': 0, '76-85': 0, '36-45': 0,
                               '26-35': 0L, '66-75': 0, 'UNSET': 0, '56-65': 0},
                   'hpo': u'PITT'}, response)

  def test_get_history_total_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time4,
                 time_fp_stored=self.time5)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=TOTAL
          &startDate=2018-01-01
          &endDate=2018-01-08
          &history=TRUE
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({u'date': u'2018-01-01', u'metrics': {u'TOTAL': 2}}, response)
    self.assertIn({u'date': u'2018-01-02', u'metrics': {u'TOTAL': 3}}, response)
    self.assertIn({u'date': u'2018-01-07', u'metrics': {u'TOTAL': 3}}, response)
    self.assertIn({u'date': u'2018-01-08', u'metrics': {u'TOTAL': 3}}, response)

  def test_get_history_total_api_filter_by_awardees(self):
    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time4,
                 time_fp_stored=self.time5)

    service = ParticipantCountsOverTimeService()
    dao = MetricsEnrollmentStatusCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=TOTAL
          &startDate=2018-01-01
          &endDate=2018-01-08
          &history=TRUE
          &awardee=AZ_TUCSON,PITT
          """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({u'date': u'2018-01-01', u'metrics': {u'TOTAL': 1}}, response)
    self.assertIn({u'date': u'2018-01-02', u'metrics': {u'TOTAL': 2}}, response)
    self.assertIn({u'date': u'2018-01-07', u'metrics': {u'TOTAL': 2}}, response)
    self.assertIn({u'date': u'2018-01-08', u'metrics': {u'TOTAL': 2}}, response)

  def test_refresh_metrics_race_cache_data(self):

    questionnaire_id = self.create_demographics_questionnaire()

    def setup_participant(when, race_code_list, providerLink=self.provider_link):
      # Set up participant, questionnaire, and consent
      with FakeClock(when):
        participant = self.send_post('Participant', {"providerLink": [providerLink]})
        participant_id = participant['participantId']
        self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
          'race': race_code_list,
          'genderIdentity': PMI_SKIP_CODE,
          'firstName': self.fake.first_name(),
          'middleName': self.fake.first_name(),
          'lastName': self.fake.last_name(),
          'zipCode': '78751',
          'state': PMI_SKIP_CODE,
          'streetAddress': '1234 Main Street',
          'city': 'Austin',
          'sex': PMI_SKIP_CODE,
          'sexualOrientation': PMI_SKIP_CODE,
          'phoneNumber': '512-555-5555',
          'recontactMethod': PMI_SKIP_CODE,
          'language': PMI_SKIP_CODE,
          'education': PMI_SKIP_CODE,
          'income': PMI_SKIP_CODE,
          'dateOfBirth': datetime.date(1978, 10, 9),
          'CABoRSignature': 'signature.pdf',
        }
      self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
      return participant

    setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
    setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
    setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)

    setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRaceCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')

    self.assertIn({'date': '2017-12-31', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 1L,
                                                     'American_Indian_Alaska_Native': 0L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, results)

    self.assertIn({'date': '2018-01-03', 'metrics': {'None_Of_These_Fully_Describe_Me': 1L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 2L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 1L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, results)

    self.assertIn({'date': '2018-01-01', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 0L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'AZ_TUCSON'}, results)

    self.assertIn({'date': '2018-01-08', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 1L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'AZ_TUCSON'}, results)

  def test_refresh_metrics_race_cache_data_for_public_metrics_api(self):

    questionnaire_id = self.create_demographics_questionnaire()

    def setup_participant(when, race_code_list, providerLink=self.provider_link):
      # Set up participant, questionnaire, and consent
      with FakeClock(when):
        participant = self.send_post('Participant', {"providerLink": [providerLink]})
        participant_id = participant['participantId']
        self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
          'race': race_code_list,
          'genderIdentity': PMI_SKIP_CODE,
          'firstName': self.fake.first_name(),
          'middleName': self.fake.first_name(),
          'lastName': self.fake.last_name(),
          'zipCode': '78751',
          'state': PMI_SKIP_CODE,
          'streetAddress': '1234 Main Street',
          'city': 'Austin',
          'sex': PMI_SKIP_CODE,
          'sexualOrientation': PMI_SKIP_CODE,
          'phoneNumber': '512-555-5555',
          'recontactMethod': PMI_SKIP_CODE,
          'language': PMI_SKIP_CODE,
          'education': PMI_SKIP_CODE,
          'income': PMI_SKIP_CODE,
          'dateOfBirth': datetime.date(1978, 10, 9),
          'CABoRSignature': 'signature.pdf',
        }
      self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
      return participant

    setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
    setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
    setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)

    setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsRaceCacheDao(MetricsCacheType.METRICS_V2_API))
    service.refresh_data_for_metrics_cache(MetricsRaceCacheDao(
      MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
    dao = MetricsRaceCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)

    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'None_Of_These_Fully_Describe_Me': 0,
                               'Middle_Eastern_North_African': 0,
                               'Multi_Ancestry': 1,
                               'American_Indian_Alaska_Native': 0,
                               'No_Ancestry_Checked': 0,
                               'Black_African_American': 0,
                               'White': 0,
                               'Prefer_Not_To_Answer': 0,
                               'Hispanic_Latino_Spanish': 0,
                               'Native_Hawaiian_other_Pacific_Islander': 0,
                               'Asian': 0}}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'None_Of_These_Fully_Describe_Me': 1,
                               'Middle_Eastern_North_African': 0,
                               'Multi_Ancestry': 1,
                               'American_Indian_Alaska_Native': 1,
                               'No_Ancestry_Checked': 0,
                               'Black_African_American': 0,
                               'White': 0,
                               'Prefer_Not_To_Answer': 0,
                               'Hispanic_Latino_Spanish': 0,
                               'Native_Hawaiian_other_Pacific_Islander': 0,
                               'Asian': 0}}, results)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'None_Of_These_Fully_Describe_Me': 1,
                               'Middle_Eastern_North_African': 0,
                               'Multi_Ancestry': 2,
                               'American_Indian_Alaska_Native': 2,
                               'No_Ancestry_Checked': 0,
                               'Black_African_American': 0,
                               'White': 0,
                               'Prefer_Not_To_Answer': 0,
                               'Hispanic_Latino_Spanish': 0,
                               'Native_Hawaiian_other_Pacific_Islander': 0,
                               'Asian': 0}}, results)

  def test_get_history_race_data_api(self):

    questionnaire_id = self.create_demographics_questionnaire()

    def setup_participant(when, race_code_list, providerLink=self.provider_link):
      # Set up participant, questionnaire, and consent
      with FakeClock(when):
        participant = self.send_post('Participant', {"providerLink": [providerLink]})
        participant_id = participant['participantId']
        self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
          'race': race_code_list,
          'genderIdentity': PMI_SKIP_CODE,
          'firstName': self.fake.first_name(),
          'middleName': self.fake.first_name(),
          'lastName': self.fake.last_name(),
          'zipCode': '78751',
          'state': PMI_SKIP_CODE,
          'streetAddress': '1234 Main Street',
          'city': 'Austin',
          'sex': PMI_SKIP_CODE,
          'sexualOrientation': PMI_SKIP_CODE,
          'phoneNumber': '512-555-5555',
          'recontactMethod': PMI_SKIP_CODE,
          'language': PMI_SKIP_CODE,
          'education': PMI_SKIP_CODE,
          'income': PMI_SKIP_CODE,
          'dateOfBirth': datetime.date(1978, 10, 9),
          'CABoRSignature': 'signature.pdf',
        }
      self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
      return participant

    setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
    setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
    setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRaceCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
              &stratification=RACE
              &startDate=2017-12-31
              &endDate=2018-01-08
              &history=TRUE
              """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2017-12-31', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 1L,
                                                     'American_Indian_Alaska_Native': 0L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, response)

    self.assertIn({'date': '2018-01-03', 'metrics': {'None_Of_These_Fully_Describe_Me': 1L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 2L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 1L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, response)

    self.assertIn({'date': '2018-01-01', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 0L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'AZ_TUCSON'}, response)

    self.assertIn({'date': '2018-01-08', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 1L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'AZ_TUCSON'}, response)

  def test_get_history_race_data_api_filter_by_awardee(self):

    questionnaire_id = self.create_demographics_questionnaire()

    def setup_participant(when, race_code_list, providerLink=self.provider_link):
      # Set up participant, questionnaire, and consent
      with FakeClock(when):
        participant = self.send_post('Participant', {"providerLink": [providerLink]})
        participant_id = participant['participantId']
        self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
          'race': race_code_list,
          'genderIdentity': PMI_SKIP_CODE,
          'firstName': self.fake.first_name(),
          'middleName': self.fake.first_name(),
          'lastName': self.fake.last_name(),
          'zipCode': '78751',
          'state': PMI_SKIP_CODE,
          'streetAddress': '1234 Main Street',
          'city': 'Austin',
          'sex': PMI_SKIP_CODE,
          'sexualOrientation': PMI_SKIP_CODE,
          'phoneNumber': '512-555-5555',
          'recontactMethod': PMI_SKIP_CODE,
          'language': PMI_SKIP_CODE,
          'education': PMI_SKIP_CODE,
          'income': PMI_SKIP_CODE,
          'dateOfBirth': datetime.date(1978, 10, 9),
          'CABoRSignature': 'signature.pdf',
        }
      self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
      return participant

    setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
    setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
    setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)

    setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRaceCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
              &stratification=RACE
              &startDate=2017-12-31
              &endDate=2018-01-08
              &history=TRUE
              &awardee=PITT
              """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2017-12-31', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 1L,
                                                     'American_Indian_Alaska_Native': 0L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, response)

    self.assertIn({'date': '2018-01-03', 'metrics': {'None_Of_These_Fully_Describe_Me': 1L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 2L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 1L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, response)

  def test_get_history_race_data_api_filter_by_enrollment_status(self):

    questionnaire_id = self.create_demographics_questionnaire()

    def setup_participant(when, race_code_list, providerLink=self.provider_link):
      # Set up participant, questionnaire, and consent
      with FakeClock(when):
        participant = self.send_post('Participant', {"providerLink": [providerLink]})
        participant_id = participant['participantId']
        self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
          'race': race_code_list,
          'genderIdentity': PMI_SKIP_CODE,
          'firstName': self.fake.first_name(),
          'middleName': self.fake.first_name(),
          'lastName': self.fake.last_name(),
          'zipCode': '78751',
          'state': PMI_SKIP_CODE,
          'streetAddress': '1234 Main Street',
          'city': 'Austin',
          'sex': PMI_SKIP_CODE,
          'sexualOrientation': PMI_SKIP_CODE,
          'phoneNumber': '512-555-5555',
          'recontactMethod': PMI_SKIP_CODE,
          'language': PMI_SKIP_CODE,
          'education': PMI_SKIP_CODE,
          'income': PMI_SKIP_CODE,
          'dateOfBirth': datetime.date(1978, 10, 9),
          'CABoRSignature': 'signature.pdf',
        }
      self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
      return participant

    p1 = setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    self.update_participant_summary(p1['participantId'][1:], time_mem=self.time2)
    p2 = setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
    self.update_participant_summary(p2['participantId'][1:], time_mem=self.time3,
                                    time_fp_stored=self.time5)
    p3 = setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
    self.update_participant_summary(p3['participantId'][1:], time_mem=self.time4)
    p4 = setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
    self.update_participant_summary(p4['participantId'][1:], time_mem=self.time5)
    p5 = setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
    self.update_participant_summary(p5['participantId'][1:], time_mem=self.time4,
                                    time_fp_stored=self.time5)

    setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
    setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRaceCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
              &stratification=RACE
              &startDate=2017-12-31
              &endDate=2018-01-08
              &history=TRUE
              &awardee=PITT
              &enrollmentStatus=MEMBER
              """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({'date': '2018-01-03', 'metrics': {'None_Of_These_Fully_Describe_Me': 1L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 2L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 0L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, response)

    self.assertIn({'date': '2018-01-04', 'metrics': {'None_Of_These_Fully_Describe_Me': 0L,
                                                     'Middle_Eastern_North_African': 0L,
                                                     'Multi_Ancestry': 1L,
                                                     'American_Indian_Alaska_Native': 1L,
                                                     'No_Ancestry_Checked': 1L,
                                                     'Black_African_American': 0L,
                                                     'White': 0L,
                                                     'Prefer_Not_To_Answer': 0L,
                                                     'Hispanic_Latino_Spanish': 0L,
                                                     'Native_Hawaiian_other_Pacific_Islander': 0L,
                                                     'Asian': 0L}, 'hpo': u'PITT'}, response)

  def test_refresh_metrics_region_cache_data(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'FULL_STATE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_STATE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'FULL_STATE')

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                              'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'}])
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'}, results2)

    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'}, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_STATE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_STATE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_STATE')

    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)

    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'FULL_CENSUS')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_CENSUS')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'FULL_CENSUS')
    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'}])
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'UNSET'}, results2)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'UNSET'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0},
                   'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'AZ_TUCSON'}, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_CENSUS')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_CENSUS')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_CENSUS')
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'FULL_AWARDEE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_AWARDEE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'FULL_AWARDEE')
    self.assertEquals(results1, [{'date': '2017-12-31', 'count': 1L, 'hpo': u'UNSET'}])
    self.assertIn({'date': '2018-01-01', 'count': 1L, 'hpo': u'UNSET'}, results2)
    self.assertIn({'date': '2018-01-01', 'count': 1L, 'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-02', 'count': 1L, 'hpo': u'UNSET'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2L, 'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2L, 'hpo': u'AZ_TUCSON'}, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_AWARDEE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_AWARDEE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_AWARDEE')
    self.assertIn({'date': '2017-12-31', 'hpo': u'UNSET', 'count': 1L}, results1)
    self.assertIn({'date': '2017-12-31', 'hpo': u'PITT', 'count': 1L}, results1)
    self.assertIn({'date': '2018-01-01', 'hpo': u'PITT', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'UNSET', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'AZ_TUCSON', 'count': 1L}, results2)

    self.assertIn({'date': '2018-01-02', 'hpo': u'UNSET', 'count': 1L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'PITT', 'count': 2L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'AZ_TUCSON', 'count': 2L}, results3)

  def test_refresh_metrics_region_cache_data_v2(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao(version=MetricsAPIVersion.V2)
    service.refresh_data_for_metrics_cache(dao)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_STATE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_STATE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_STATE')

    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)

    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_CENSUS')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_CENSUS')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_CENSUS')
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_AWARDEE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_AWARDEE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_AWARDEE')
    self.assertIn({'date': '2017-12-31', 'hpo': u'UNSET', 'count': 1L}, results1)
    self.assertIn({'date': '2017-12-31', 'hpo': u'PITT', 'count': 1L}, results1)
    self.assertIn({'date': '2018-01-01', 'hpo': u'PITT', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'UNSET', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'AZ_TUCSON', 'count': 1L}, results2)

    self.assertIn({'date': '2018-01-02', 'hpo': u'UNSET', 'count': 1L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'PITT', 'count': 2L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'AZ_TUCSON', 'count': 2L}, results3)

  def test_refresh_metrics_region_cache_data_for_public_metrics_api(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
    service.refresh_data_for_metrics_cache(dao)

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'GEO_STATE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_CENSUS')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'GEO_AWARDEE')
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1, 'GA': 0,
                               'IN': 1, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)
    self.assertIn({'date': '2018-01-01',
                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 3,
                              'SOUTH': 0}}, results2)
    self.assertIn({'date': '2018-01-02', 'count': 1, 'hpo': u'UNSET'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2, 'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2, 'hpo': u'AZ_TUCSON'}, results3)

  def test_get_metrics_region_data_api(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_fp=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs1 = """
                  &stratification=FULL_STATE
                  &endDate=2017-12-31
                  &history=TRUE
                  """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                      &stratification=FULL_STATE
                      &endDate=2018-01-01
                      &history=TRUE
                      """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                          &stratification=FULL_STATE
                          &endDate=2018-01-02
                          &history=TRUE
                          """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                              'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'}])
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'}, results2)

    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'}, results3)

    qs1 = """
                      &stratification=GEO_STATE
                      &endDate=2017-12-31
                      &history=TRUE
                      &enrollmentStatus=INTERESTED,FULL_PARTICIPANT
                      """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                          &stratification=GEO_STATE
                          &endDate=2018-01-01
                          &history=TRUE
                          """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                              &stratification=GEO_STATE
                              &endDate=2018-01-02
                              &history=TRUE
                              """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)

    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)

    qs1 = """
                      &stratification=FULL_CENSUS
                      &endDate=2017-12-31
                      &history=TRUE
                      """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                          &stratification=FULL_CENSUS
                          &endDate=2018-01-01
                          &history=TRUE
                          """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                              &stratification=FULL_CENSUS
                              &endDate=2018-01-02
                              &history=TRUE
                              """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'}])
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'UNSET'}, results2)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'UNSET'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0},
                   'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'AZ_TUCSON'}, results3)

    qs1 = """
                          &stratification=GEO_CENSUS
                          &endDate=2017-12-31
                          &history=TRUE
                          """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=GEO_CENSUS
                              &endDate=2018-01-01
                              &history=TRUE
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                  &stratification=GEO_CENSUS
                                  &endDate=2018-01-02
                                  &history=TRUE
                                  """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)

    qs1 = """
                          &stratification=FULL_AWARDEE
                          &endDate=2017-12-31
                          &history=TRUE
                          """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=FULL_AWARDEE
                              &endDate=2018-01-01
                              &history=TRUE
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                  &stratification=FULL_AWARDEE
                                  &endDate=2018-01-02
                                  &history=TRUE
                                  """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31', 'count': 1L, 'hpo': u'UNSET'}])
    self.assertIn({'date': '2018-01-01', 'count': 1L, 'hpo': u'UNSET'}, results2)
    self.assertIn({'date': '2018-01-01', 'count': 1L, 'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-02', 'count': 1L, 'hpo': u'UNSET'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2L, 'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2L, 'hpo': u'AZ_TUCSON'}, results3)

    qs1 = """
                              &stratification=GEO_AWARDEE
                              &endDate=2017-12-31
                              &history=TRUE
                              """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                                  &stratification=GEO_AWARDEE
                                  &endDate=2018-01-01
                                  &history=TRUE
                                  """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                      &stratification=GEO_AWARDEE
                                      &endDate=2018-01-02
                                      &history=TRUE
                                      """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertIn({'date': '2017-12-31', 'hpo': u'UNSET', 'count': 1L}, results1)
    self.assertIn({'date': '2017-12-31', 'hpo': u'PITT', 'count': 1L}, results1)
    self.assertIn({'date': '2018-01-01', 'hpo': u'PITT', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'UNSET', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'AZ_TUCSON', 'count': 1L}, results2)

    self.assertIn({'date': '2018-01-02', 'hpo': u'UNSET', 'count': 1L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'PITT', 'count': 2L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'AZ_TUCSON', 'count': 2L}, results3)

  def test_get_metrics_region_data_api_v2(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_fp=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs1 = """
                      &stratification=GEO_STATE
                      &endDate=2017-12-31
                      &history=TRUE
                      &enrollmentStatus=PARTICIPANT,CORE_PARTICIPANT
                      &version=2
                      """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                          &stratification=GEO_STATE
                          &endDate=2018-01-01
                          &history=TRUE
                          &version=2
                          """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                              &stratification=GEO_STATE
                              &endDate=2018-01-02
                              &history=TRUE
                              &version=2
                              """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results1)

    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                               'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)

    qs1 = """
                          &stratification=GEO_CENSUS
                          &endDate=2017-12-31
                          &history=TRUE
                          &version=2
                          """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=GEO_CENSUS
                              &endDate=2018-01-01
                              &history=TRUE
                              &version=2
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                  &stratification=GEO_CENSUS
                                  &endDate=2018-01-02
                                  &history=TRUE
                                  &version=2
                                  """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertIn({'date': '2017-12-31',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2017-12-31',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results1)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'UNSET',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)

    qs1 = """
                              &stratification=GEO_AWARDEE
                              &endDate=2017-12-31
                              &history=TRUE
                              &version=2
                              """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                                  &stratification=GEO_AWARDEE
                                  &endDate=2018-01-01
                                  &history=TRUE
                                  &version=2
                                  """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                      &stratification=GEO_AWARDEE
                                      &endDate=2018-01-02
                                      &history=TRUE
                                      &version=2
                                      """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertIn({'date': '2017-12-31', 'hpo': u'UNSET', 'count': 1L}, results1)
    self.assertIn({'date': '2017-12-31', 'hpo': u'PITT', 'count': 1L}, results1)
    self.assertIn({'date': '2018-01-01', 'hpo': u'PITT', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'UNSET', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01', 'hpo': u'AZ_TUCSON', 'count': 1L}, results2)

    self.assertIn({'date': '2018-01-02', 'hpo': u'UNSET', 'count': 1L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'PITT', 'count': 2L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'AZ_TUCSON', 'count': 2L}, results3)

  def test_get_metrics_region_data_api_filter_by_enrollment_status(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_fp=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
                      &stratification=GEO_STATE
                      &endDate=2017-12-31
                      &history=TRUE
                      &enrollmentStatus=MEMBER,FULL_PARTICIPANT
                      """

    qs = ''.join(qs.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertEquals(results, [{'date': '2017-12-31',
                                 'hpo': u'UNSET',
                                 'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                             'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                             'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                             'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                             'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                             'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                             'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                             'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                             'KY': 0, 'OR': 0, 'SD': 0}}])

    qs = """
                          &stratification=GEO_CENSUS
                          &endDate=2017-12-31
                          &history=TRUE
                          &enrollmentStatus=MEMBER,FULL_PARTICIPANT
                          """

    qs = ''.join(qs.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertEquals(results, [{'date': '2017-12-31',
                                 'hpo': u'UNSET',
                                 'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                                 }])

    qs = """
                              &stratification=GEO_AWARDEE
                              &endDate=2017-12-31
                              &history=TRUE
                              &enrollmentStatus=MEMBER,FULL_PARTICIPANT
                              """

    qs = ''.join(qs.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertEquals(results, [{'date': '2017-12-31', 'hpo': u'UNSET', 'count': 1L}])

  def test_get_metrics_region_data_api_filter_by_enrollment_status_v2(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_fp=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs = """
          &stratification=GEO_STATE
          &endDate=2017-12-31
          &history=TRUE
          &enrollmentStatus=PARTICIPANT,CORE_PARTICIPANT
          &version=2
         """

    qs = ''.join(qs.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({u'date': u'2017-12-31',
                   u'metrics': {u'WA': 0, u'DE': 0, u'DC': 0, u'WI': 0, u'WV': 0, u'HI': 0,
                                u'FL': 0, u'WY': 0, u'NH': 0, u'NJ': 0, u'NM': 0, u'TX': 0,
                                u'LA': 0, u'NC': 0, u'ND': 0, u'NE': 0, u'TN': 0, u'NY': 0,
                                u'PA': 0, u'RI': 0, u'NV': 0, u'VA': 0, u'CO': 0, u'AK': 0,
                                u'AL': 0, u'AR': 0, u'VT': 0, u'IL': 1, u'GA': 0, u'IN': 0,
                                u'IA': 0, u'OK': 0, u'AZ': 0, u'CA': 0, u'ID': 0, u'CT': 0,
                                u'ME': 0, u'MD': 0, u'MA': 0, u'OH': 0, u'UT': 0, u'MO': 0,
                                u'MN': 0, u'MI': 0, u'KS': 0, u'MT': 0, u'MS': 0, u'SC': 0,
                                u'KY': 0, u'OR': 0, u'SD': 0}, u'hpo': u'UNSET'}, results)
    self.assertIn({u'date': u'2017-12-31',
                   u'metrics': {u'WA': 0, u'DE': 0, u'DC': 0, u'WI': 0, u'WV': 0, u'HI': 0,
                                u'FL': 0, u'WY': 0, u'NH': 0, u'NJ': 0, u'NM': 0, u'TX': 0,
                                u'LA': 0, u'NC': 0, u'ND': 0, u'NE': 0, u'TN': 0, u'NY': 0,
                                u'PA': 0, u'RI': 0, u'NV': 0, u'VA': 0, u'CO': 0, u'AK': 0,
                                u'AL': 0, u'AR': 0, u'VT': 0, u'IL': 0, u'GA': 0, u'IN': 1,
                                u'IA': 0, u'OK': 0, u'AZ': 0, u'CA': 0, u'ID': 0, u'CT': 0,
                                u'ME': 0, u'MD': 0, u'MA': 0, u'OH': 0, u'UT': 0, u'MO': 0,
                                u'MN': 0, u'MI': 0, u'KS': 0, u'MT': 0, u'MS': 0, u'SC': 0,
                                u'KY': 0, u'OR': 0, u'SD': 0}, u'hpo': u'PITT'}, results)

    qs = """
          &stratification=GEO_CENSUS
          &endDate=2017-12-31
          &history=TRUE
          &enrollmentStatus=PARTICIPANT,CORE_PARTICIPANT
          &version=2
         """

    qs = ''.join(qs.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({u'date': u'2017-12-31',
                   u'metrics': {u'WEST': 0, u'NORTHEAST': 0, u'MIDWEST': 1, u'SOUTH': 0},
                   u'hpo': u'UNSET'}, results)
    self.assertIn({u'date': u'2017-12-31',
                   u'metrics': {u'WEST': 0, u'NORTHEAST': 0, u'MIDWEST': 1, u'SOUTH': 0},
                   u'hpo': u'PITT'}, results)

    qs = """
          &stratification=GEO_AWARDEE
          &endDate=2017-12-31
          &history=TRUE
          &enrollmentStatus=PARTICIPANT,CORE_PARTICIPANT
          &awardee=PITT,AZ_TUCSON
          &version=2
         """

    qs = ''.join(qs.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertIn({u'date': u'2017-12-31', u'count': 1, u'hpo': u'PITT'}, results)

  def test_get_metrics_region_data_api_filter_by_awardee(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_fp=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs1 = """
                  &stratification=FULL_STATE
                  &endDate=2017-12-31
                  &history=TRUE
                  &awardee=PITT,AZ_TUCSON
                  """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                      &stratification=FULL_STATE
                      &endDate=2018-01-01
                      &history=TRUE
                      &awardee=PITT,AZ_TUCSON
                      """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                          &stratification=FULL_STATE
                          &endDate=2018-01-02
                          &history=TRUE
                          &awardee=PITT,AZ_TUCSON
                          """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [])
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'PITT'}, results3)

    qs1 = """
                          &stratification=GEO_STATE
                          &endDate=2017-12-31
                          &history=TRUE
                          &awardee=PITT,AZ_TUCSON
                          """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=GEO_STATE
                              &endDate=2018-01-01
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                  &stratification=GEO_STATE
                                  &endDate=2018-01-02
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'hpo': u'PITT',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}}])
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)

    qs1 = """
                      &stratification=FULL_CENSUS
                      &endDate=2017-12-31
                      &history=TRUE
                      &awardee=PITT,AZ_TUCSON
                      """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                          &stratification=FULL_CENSUS
                          &endDate=2018-01-01
                          &history=TRUE
                          &awardee=PITT,AZ_TUCSON
                          """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                              &stratification=FULL_CENSUS
                              &endDate=2018-01-02
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [])
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0},
                   'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                   'hpo': u'AZ_TUCSON'}, results3)

    qs1 = """
                              &stratification=GEO_CENSUS
                              &endDate=2017-12-31
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                                  &stratification=GEO_CENSUS
                                  &endDate=2018-01-01
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                      &stratification=GEO_CENSUS
                                      &endDate=2018-01-02
                                      &history=TRUE
                                      &awardee=PITT,AZ_TUCSON
                                      """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'hpo': u'PITT',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                                  }])
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)

    qs1 = """
                          &stratification=FULL_AWARDEE
                          &endDate=2017-12-31
                          &history=TRUE
                          &awardee=PITT,AZ_TUCSON
                          """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=FULL_AWARDEE
                              &endDate=2018-01-01
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                  &stratification=FULL_AWARDEE
                                  &endDate=2018-01-02
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [])
    self.assertEquals(results2, [{'date': '2018-01-01', 'count': 1L, 'hpo': u'AZ_TUCSON'}])
    self.assertIn({'date': '2018-01-02', 'count': 2L, 'hpo': u'PITT'}, results3)
    self.assertIn({'date': '2018-01-02', 'count': 2L, 'hpo': u'AZ_TUCSON'}, results3)

    qs1 = """
                                  &stratification=GEO_AWARDEE
                                  &endDate=2017-12-31
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                                      &stratification=GEO_AWARDEE
                                      &endDate=2018-01-01
                                      &history=TRUE
                                      &awardee=PITT,AZ_TUCSON
                                      """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                          &stratification=GEO_AWARDEE
                                          &endDate=2018-01-02
                                          &history=TRUE
                                          &awardee=PITT,AZ_TUCSON
                                          """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31', 'hpo': u'PITT', 'count': 1L}
                                 ])
    self.assertIn({'date': '2018-01-01', 'hpo': u'PITT', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01',  'hpo': u'AZ_TUCSON', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-02', 'hpo': u'PITT', 'count': 2L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'AZ_TUCSON', 'count': 2L}, results3)

  def test_get_metrics_region_data_api_filter_by_awardee_v2(self):

    code1 = Code(codeId=1, system="a", value="PIIState_IL", display=u"PIIState_IL", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=3, system="c", value="PIIState_CA", display=u"PIIState_CA", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time3,
                 time_mem=self.time3, time_fp_stored=self.time3, state_id=2)

    p5 = Participant(participantId=6, biobankId=9)
    self._insert(p5, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_fp=self.time1, time_fp_stored=self.time1, state_id=1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    qs1 = """
                          &stratification=GEO_STATE
                          &endDate=2017-12-31
                          &history=TRUE
                          &awardee=PITT,AZ_TUCSON
                          &version=2
                          """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=GEO_STATE
                              &endDate=2018-01-01
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              &version=2
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                  &stratification=GEO_STATE
                                  &endDate=2018-01-02
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  &version=2
                                  """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'hpo': u'PITT',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}}])
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                               'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                               'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                               'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                               'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                               'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                               'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                               'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                               'KY': 0, 'OR': 0, 'SD': 0}}, results3)

    qs1 = """
                              &stratification=GEO_CENSUS
                              &endDate=2017-12-31
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              &version=2
                              """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                                  &stratification=GEO_CENSUS
                                  &endDate=2018-01-01
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  &version=2
                                  """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                      &stratification=GEO_CENSUS
                                      &endDate=2018-01-02
                                      &history=TRUE
                                      &awardee=PITT,AZ_TUCSON
                                      &version=2
                                      """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'hpo': u'PITT',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                                  }])
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-01',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results2)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'PITT',
                   'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0}
                   }, results3)
    self.assertIn({'date': '2018-01-02',
                   'hpo': u'AZ_TUCSON',
                   'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0}
                   }, results3)

    qs1 = """
                                  &stratification=GEO_AWARDEE
                                  &endDate=2017-12-31
                                  &history=TRUE
                                  &awardee=PITT,AZ_TUCSON
                                  &version=2
                                  """

    qs1 = ''.join(qs1.split())
    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                                      &stratification=GEO_AWARDEE
                                      &endDate=2018-01-01
                                      &history=TRUE
                                      &awardee=PITT,AZ_TUCSON
                                      &version=2
                                      """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    qs3 = """
                                          &stratification=GEO_AWARDEE
                                          &endDate=2018-01-02
                                          &history=TRUE
                                          &awardee=PITT,AZ_TUCSON
                                          &version=2
                                          """

    qs3 = ''.join(qs3.split())

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertEquals(results1, [{'date': '2017-12-31', 'hpo': u'PITT', 'count': 1L}
                                 ])
    self.assertIn({'date': '2018-01-01', 'hpo': u'PITT', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-01',  'hpo': u'AZ_TUCSON', 'count': 1L}, results2)
    self.assertIn({'date': '2018-01-02', 'hpo': u'PITT', 'count': 2L}, results3)
    self.assertIn({'date': '2018-01-02', 'hpo': u'AZ_TUCSON', 'count': 2L}, results3)

  def test_unrecognized_state_value(self):
    # PW is not in the state list
    code1 = Code(codeId=1, system="a", value="PIIState_PW", display=u"PIIState_PW", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=2, system="b", value="PIIState_IN", display=u"PIIState_IN", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time1,
                 time_mem=self.time2, time_fp_stored=self.time2, state_id=2)

    service = ParticipantCountsOverTimeService()
    dao = MetricsRegionCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    results1 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_STATE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_STATE')
    results3 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_CENSUS')
    results4 = dao.get_latest_version_from_cache('2018-01-01', 'GEO_CENSUS')

    self.assertEquals(results1, [{'date': '2018-01-01',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'hpo': u'AZ_TUCSON',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}}])
    self.assertEquals(results3, [{'date': '2018-01-01',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results4, [{'date': '2018-01-01',  'hpo': u'AZ_TUCSON',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L,
                                              'SOUTH': 0}}])

  def test_refresh_metrics_lifecycle_cache_data(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp=self.time3, time_fp_stored=self.time3)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time5, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsLifecycleCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    results = dao.get_latest_version_from_cache('2018-01-03')
    self.assertEquals(results, [{'date': '2018-01-03',
                                 'metrics': {'not_completed': {'Full_Participant': 0L,
                                                               'Baseline_PPI_Modules_Complete': 0L,
                                                               'PPI_Module_The_Basics': 0L,
                                                               'Consent_Complete': 0L,
                                                               'PPI_Module_Overall_Health': 0L,
                                                               'Consent_Enrollment': 0L,
                                                               'PPI_Module_Lifestyle': 0L,
                                                               'Physical_Measurements': 0L,
                                                               'Registered': 0,
                                                               'Samples_Received': 0L},
                                             'completed': {'Full_Participant': 1L,
                                                           'Baseline_PPI_Modules_Complete': 1L,
                                                           'PPI_Module_The_Basics': 1L,
                                                           'Consent_Complete': 1L,
                                                           'PPI_Module_Overall_Health': 1L,
                                                           'Consent_Enrollment': 1L,
                                                           'PPI_Module_Lifestyle': 1L,
                                                           'Physical_Measurements': 1L,
                                                           'Registered': 1L,
                                                           'Samples_Received': 1L}
                                             }, 'hpo': u'UNSET'},
                                {'date': '2018-01-03',
                                 'metrics': {'not_completed': {'Full_Participant': 2L,
                                                               'Baseline_PPI_Modules_Complete': 1L,
                                                               'PPI_Module_The_Basics': 1L,
                                                               'Consent_Complete': 1L,
                                                               'PPI_Module_Overall_Health': 1L,
                                                               'Consent_Enrollment': 0L,
                                                               'PPI_Module_Lifestyle': 1L,
                                                               'Physical_Measurements': 1L,
                                                               'Registered': 0,
                                                               'Samples_Received': 1L},
                                             'completed': {'Full_Participant': 0L,
                                                           'Baseline_PPI_Modules_Complete': 1L,
                                                           'PPI_Module_The_Basics': 1L,
                                                           'Consent_Complete': 1L,
                                                           'PPI_Module_Overall_Health': 1L,
                                                           'Consent_Enrollment': 2L,
                                                           'PPI_Module_Lifestyle': 1L,
                                                           'Physical_Measurements': 1L,
                                                           'Registered': 2L,
                                                           'Samples_Received': 1L}
                                             }, 'hpo': u'PITT'},
                                {'date': '2018-01-03',
                                 'metrics': {'not_completed': {'Full_Participant': 1L,
                                                               'Baseline_PPI_Modules_Complete': 1L,
                                                               'PPI_Module_The_Basics': 1L,
                                                               'Consent_Complete': 0L,
                                                               'PPI_Module_Overall_Health': 1L,
                                                               'Consent_Enrollment': 0L,
                                                               'PPI_Module_Lifestyle': 1L,
                                                               'Physical_Measurements': 1L,
                                                               'Registered': 0,
                                                               'Samples_Received': 1L},
                                             'completed': {'Full_Participant': 1L,
                                                           'Baseline_PPI_Modules_Complete': 1L,
                                                           'PPI_Module_The_Basics': 1L,
                                                           'Consent_Complete': 2L,
                                                           'PPI_Module_Overall_Health': 1L,
                                                           'Consent_Enrollment': 2L,
                                                           'PPI_Module_Lifestyle': 1L,
                                                           'Physical_Measurements': 1L,
                                                           'Registered': 2L,
                                                           'Samples_Received': 1L}
                                             }, 'hpo': u'AZ_TUCSON'}
                                ])

    results2 = dao.get_latest_version_from_cache('2018-01-08')
    self.assertEquals(results2, [{'date': '2018-01-08',
                                  'metrics': {'not_completed': {'Full_Participant': 0L,
                                                                'Baseline_PPI_Modules_Complete': 0L,
                                                                'PPI_Module_The_Basics': 0L,
                                                                'Consent_Complete': 0L,
                                                                'PPI_Module_Overall_Health': 0L,
                                                                'Consent_Enrollment': 0L,
                                                                'PPI_Module_Lifestyle': 0L,
                                                                'Physical_Measurements': 0L,
                                                                'Registered': 0,
                                                                'Samples_Received': 0L},
                                              'completed': {'Full_Participant': 1L,
                                                            'Baseline_PPI_Modules_Complete': 1L,
                                                            'PPI_Module_The_Basics': 1L,
                                                            'Consent_Complete': 1L,
                                                            'PPI_Module_Overall_Health': 1L,
                                                            'Consent_Enrollment': 1L,
                                                            'PPI_Module_Lifestyle': 1L,
                                                            'Physical_Measurements': 1L,
                                                            'Registered': 1L,
                                                            'Samples_Received': 1L}
                                              }, 'hpo': u'UNSET'},
                                 {'date': '2018-01-08',
                                  'metrics': {'not_completed': {'Full_Participant': 0L,
                                                                'Baseline_PPI_Modules_Complete': 0L,
                                                                'PPI_Module_The_Basics': 0L,
                                                                'Consent_Complete': 0L,
                                                                'PPI_Module_Overall_Health': 0L,
                                                                'Consent_Enrollment': 0L,
                                                                'PPI_Module_Lifestyle': 0L,
                                                                'Physical_Measurements': 0L,
                                                                'Registered': 0,
                                                                'Samples_Received': 0L},
                                              'completed': {'Full_Participant': 2L,
                                                            'Baseline_PPI_Modules_Complete': 2L,
                                                            'PPI_Module_The_Basics': 2L,
                                                            'Consent_Complete': 2L,
                                                            'PPI_Module_Overall_Health': 2L,
                                                            'Consent_Enrollment': 2L,
                                                            'PPI_Module_Lifestyle': 2L,
                                                            'Physical_Measurements': 2L,
                                                            'Registered': 2L,
                                                            'Samples_Received': 2L}
                                              }, 'hpo': u'PITT'},
                                 {'date': '2018-01-08',
                                  'metrics': {'not_completed': {'Full_Participant': 0L,
                                                                'Baseline_PPI_Modules_Complete': 0L,
                                                                'PPI_Module_The_Basics': 0L,
                                                                'Consent_Complete': 0L,
                                                                'PPI_Module_Overall_Health': 0L,
                                                                'Consent_Enrollment': 0L,
                                                                'PPI_Module_Lifestyle': 0L,
                                                                'Physical_Measurements': 0L,
                                                                'Registered': 0,
                                                                'Samples_Received': 0L},
                                              'completed': {'Full_Participant': 2L,
                                                            'Baseline_PPI_Modules_Complete': 2L,
                                                            'PPI_Module_The_Basics': 2L,
                                                            'Consent_Complete': 2L,
                                                            'PPI_Module_Overall_Health': 2L,
                                                            'Consent_Enrollment': 2L,
                                                            'PPI_Module_Lifestyle': 2L,
                                                            'Physical_Measurements': 2L,
                                                            'Registered': 2L, 'Samples_Received': 2L}
                                              }, 'hpo': u'AZ_TUCSON'}])

  def test_refresh_metrics_lifecycle_cache_data_v2(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp=self.time3, time_fp_stored=self.time3)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time5, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time4, time_fp_stored=self.time5)

    p5 = Participant(participantId=7, biobankId=10)
    self._insert(p5, 'Chad4', 'Caterpillar4', 'PITT', time_int=self.time0, time_study=self.time0,
                 time_mem=self.time0, time_fp=self.time0, time_fp_stored=self.time0)

    p6 = Participant(participantId=8, biobankId=11)
    ppi_modules = dict(
      questionnaireOnTheBasicsTime=self.time0,
      questionnaireOnLifestyleTime=self.time0,
      questionnaireOnOverallHealthTime=self.time0,
      questionnaireOnHealthcareAccessTime=self.time0,
      questionnaireOnMedicalHistoryTime=self.time0,
      questionnaireOnMedicationsTime=self.time0,
      questionnaireOnFamilyHealthTime=self.time5
    )
    self._insert(p6, 'Chad5', 'Caterpillar5', 'PITT', time_int=self.time0, time_study=self.time0,
                 time_mem=self.time0, time_fp=self.time0, time_fp_stored=self.time0, **ppi_modules)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    service = ParticipantCountsOverTimeService()
    dao = MetricsLifecycleCacheDao(MetricsCacheType.METRICS_V2_API, MetricsAPIVersion.V2)
    service.refresh_data_for_metrics_cache(dao)

    results = dao.get_latest_version_from_cache('2018-01-01')
    self.assertEquals(results, [{'date': '2018-01-01',
                                 'metrics': {'not_completed':
                                               {'Full_Participant': 0L,
                                                'PPI_Module_The_Basics': 0L,
                                                'Consent_Complete': 0L,
                                                'Consent_Enrollment': 0L,
                                                'PPI_Module_Lifestyle': 0L,
                                                'Baseline_PPI_Modules_Complete': 0L,
                                                'PPI_Module_Family_Health': 0L,
                                                'PPI_Module_Overall_Health': 0L,
                                                'PPI_Retention_Modules_Complete': 0L,
                                                'Physical_Measurements': 0L,
                                                'Registered': 0,
                                                'PPI_Module_Medical_History': 0L,
                                                'PPI_Module_Healthcare_Access': 0L,
                                                'Samples_Received': 0L},
                                             'completed':
                                               {'Full_Participant': 1L,
                                                'PPI_Module_The_Basics': 1L,
                                                'Consent_Complete': 1L,
                                                'Consent_Enrollment': 1L,
                                                'PPI_Module_Lifestyle': 1L,
                                                'Baseline_PPI_Modules_Complete': 1L,
                                                'PPI_Module_Family_Health': 0L,
                                                'PPI_Module_Overall_Health': 1L,
                                                'PPI_Retention_Modules_Complete': 0L,
                                                'Physical_Measurements': 1L,
                                                'Registered': 1L,
                                                'PPI_Module_Medical_History': 0L,
                                                'PPI_Module_Healthcare_Access': 0L,
                                                'Samples_Received': 1L}}, 'hpo': u'UNSET'},
                                {'date': '2018-01-01',
                                 'metrics': {'not_completed':
                                               {'Full_Participant': 0L,
                                                'PPI_Module_The_Basics': 0L,
                                                'Consent_Complete': 0L,
                                                'Consent_Enrollment': 0L,
                                                'PPI_Module_Lifestyle': 0L,
                                                'Baseline_PPI_Modules_Complete': 0L,
                                                'PPI_Module_Family_Health': 0L,
                                                'PPI_Module_Overall_Health': 0L,
                                                'PPI_Retention_Modules_Complete': 0L,
                                                'Physical_Measurements': 0L,
                                                'Registered': 0,
                                                'PPI_Module_Medical_History': 0L,
                                                'PPI_Module_Healthcare_Access': 0L,
                                                'Samples_Received': 0L},
                                             'completed':
                                               {'Full_Participant': 2L,
                                                'PPI_Module_The_Basics': 2L,
                                                'Consent_Complete': 2L,
                                                'Consent_Enrollment': 2L,
                                                'PPI_Module_Lifestyle': 2L,
                                                'Baseline_PPI_Modules_Complete': 2L,
                                                'PPI_Module_Family_Health': 0L,
                                                'PPI_Module_Overall_Health': 2L,
                                                'PPI_Retention_Modules_Complete': 0L,
                                                'Physical_Measurements': 2L,
                                                'Registered': 2L,
                                                'PPI_Module_Medical_History': 0L,
                                                'PPI_Module_Healthcare_Access': 0L,
                                                'Samples_Received': 2L}}, 'hpo': u'PITT'},
                                {'date': '2018-01-01',
                                 'metrics': {'not_completed':
                                               {'Full_Participant': 1L,
                                                'PPI_Module_The_Basics': 1L,
                                                'Consent_Complete': 0L,
                                                'Consent_Enrollment': 0L,
                                                'PPI_Module_Lifestyle': 1L,
                                                'Baseline_PPI_Modules_Complete': 1L,
                                                'PPI_Module_Family_Health': 0L,
                                                'PPI_Module_Overall_Health': 1L,
                                                'PPI_Retention_Modules_Complete': 0L,
                                                'Physical_Measurements': 1L,
                                                'Registered': 0,
                                                'PPI_Module_Medical_History': 0L,
                                                'PPI_Module_Healthcare_Access': 0L,
                                                'Samples_Received': 1L},
                                             'completed':
                                               {'Full_Participant': 0L,
                                                'PPI_Module_The_Basics': 0L,
                                                'Consent_Complete': 1L,
                                                'Consent_Enrollment': 1L,
                                                'PPI_Module_Lifestyle': 0L,
                                                'Baseline_PPI_Modules_Complete': 0L,
                                                'PPI_Module_Family_Health': 0L,
                                                'PPI_Module_Overall_Health': 0L,
                                                'PPI_Retention_Modules_Complete': 0L,
                                                'Physical_Measurements': 0L,
                                                'Registered': 1L,
                                                'PPI_Module_Medical_History': 0L,
                                                'PPI_Module_Healthcare_Access': 0L,
                                                'Samples_Received': 0L}}, 'hpo': u'AZ_TUCSON'}])

    results2 = dao.get_latest_version_from_cache('2018-01-03')
    self.assertEquals(results2, [{'date': '2018-01-03',
                                  'metrics': {
                                    'not_completed':
                                      {'Full_Participant': 0L,
                                       'PPI_Module_The_Basics': 0L,
                                       'Consent_Complete': 0L,
                                       'Consent_Enrollment': 0L,
                                       'PPI_Module_Lifestyle': 0L,
                                       'Baseline_PPI_Modules_Complete': 0L,
                                       'PPI_Module_Family_Health': 0L,
                                       'PPI_Module_Overall_Health': 0L,
                                       'PPI_Retention_Modules_Complete': 0L,
                                       'Physical_Measurements': 0L,
                                       'Registered': 0,
                                       'PPI_Module_Medical_History': 0L,
                                       'PPI_Module_Healthcare_Access': 0L,
                                       'Samples_Received': 0L},
                                    'completed':
                                      {'Full_Participant': 1L,
                                       'PPI_Module_The_Basics': 1L,
                                       'Consent_Complete': 1L,
                                       'Consent_Enrollment': 1L,
                                       'PPI_Module_Lifestyle': 1L,
                                       'Baseline_PPI_Modules_Complete': 1L,
                                       'PPI_Module_Family_Health': 0L,
                                       'PPI_Module_Overall_Health': 1L,
                                       'PPI_Retention_Modules_Complete': 0L,
                                       'Physical_Measurements': 1L,
                                       'Registered': 1L,
                                       'PPI_Module_Medical_History': 0L,
                                       'PPI_Module_Healthcare_Access': 0L,
                                       'Samples_Received': 1L}}, 'hpo': u'UNSET'},
                                 {'date': '2018-01-03',
                                  'metrics': {
                                    'not_completed':
                                      {'Full_Participant': 2L,
                                       'PPI_Module_The_Basics': 1L,
                                       'Consent_Complete': 1L,
                                       'Consent_Enrollment': 0L,
                                       'PPI_Module_Lifestyle': 1L,
                                       'Baseline_PPI_Modules_Complete': 1L,
                                       'PPI_Module_Family_Health': 1L,
                                       'PPI_Module_Overall_Health': 1L,
                                       'PPI_Retention_Modules_Complete': 1L,
                                       'Physical_Measurements': 1L,
                                       'Registered': 0,
                                       'PPI_Module_Medical_History': 0L,
                                       'PPI_Module_Healthcare_Access': 0L,
                                       'Samples_Received': 1L},
                                    'completed':
                                      {'Full_Participant': 2L,
                                       'PPI_Module_The_Basics': 3L,
                                       'Consent_Complete': 3L,
                                       'Consent_Enrollment': 4L,
                                       'PPI_Module_Lifestyle': 3L,
                                       'Baseline_PPI_Modules_Complete': 3L,
                                       'PPI_Module_Family_Health': 1L,
                                       'PPI_Module_Overall_Health': 3L,
                                       'PPI_Retention_Modules_Complete': 1L,
                                       'Physical_Measurements': 3L,
                                       'Registered': 4L,
                                       'PPI_Module_Medical_History': 2L,
                                       'PPI_Module_Healthcare_Access': 2L,
                                       'Samples_Received': 3L}}, 'hpo': u'PITT'},
                                 {'date': '2018-01-03',
                                  'metrics':
                                    {'not_completed':
                                       {'Full_Participant': 1L,
                                        'PPI_Module_The_Basics': 1L,
                                        'Consent_Complete': 0L,
                                        'Consent_Enrollment': 0L,
                                        'PPI_Module_Lifestyle': 1L,
                                        'Baseline_PPI_Modules_Complete': 1L,
                                        'PPI_Module_Family_Health': 0L,
                                        'PPI_Module_Overall_Health': 1L,
                                        'PPI_Retention_Modules_Complete': 0L,
                                        'Physical_Measurements': 1L,
                                        'Registered': 0,
                                        'PPI_Module_Medical_History': 0L,
                                        'PPI_Module_Healthcare_Access': 0L,
                                        'Samples_Received': 1L},
                                     'completed':
                                       {'Full_Participant': 1L,
                                        'PPI_Module_The_Basics': 1L,
                                        'Consent_Complete': 2L,
                                        'Consent_Enrollment': 2L,
                                        'PPI_Module_Lifestyle': 1L,
                                        'Baseline_PPI_Modules_Complete': 1L,
                                        'PPI_Module_Family_Health': 0L,
                                        'PPI_Module_Overall_Health': 1L,
                                        'PPI_Retention_Modules_Complete': 0L,
                                        'Physical_Measurements': 1L,
                                        'Registered': 2L,
                                        'PPI_Module_Medical_History': 0L,
                                        'PPI_Module_Healthcare_Access': 0L,
                                        'Samples_Received': 1L}}, 'hpo': u'AZ_TUCSON'}])

  def test_get_metrics_lifecycle_data_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp=self.time3, time_fp_stored=self.time3)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time5, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType
                                                                    .METRICS_V2_API))
    service.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType
                                                                    .PUBLIC_METRICS_EXPORT_API))

    qs1 = """
                          &stratification=LIFECYCLE
                          &endDate=2018-01-03
                          &history=TRUE
                          """

    qs1 = ''.join(qs1.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    qs2 = """
                              &stratification=LIFECYCLE
                              &endDate=2018-01-08
                              &history=TRUE
                              &awardee=PITT,AZ_TUCSON
                              """

    qs2 = ''.join(qs2.split())

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    self.assertEquals(results,
                      [{u'date': u'2018-01-03',
                        u'metrics': {u'not_completed': {u'Full_Participant': 0,
                                                        u'PPI_Module_The_Basics': 0,
                                                        u'Consent_Complete': 0,
                                                        u'Consent_Enrollment': 0,
                                                        u'PPI_Module_Lifestyle': 0,
                                                        u'Registered': 0,
                                                        u'Baseline_PPI_Modules_Complete': 0,
                                                        u'PPI_Module_Overall_Health': 0,
                                                        u'Physical_Measurements': 0,
                                                        u'Samples_Received': 0},
                                     u'completed': {u'Full_Participant': 1,
                                                    u'PPI_Module_The_Basics': 1,
                                                    u'Consent_Complete': 1,
                                                    u'Consent_Enrollment': 1,
                                                    u'PPI_Module_Lifestyle': 1,
                                                    u'Registered': 1,
                                                    u'Baseline_PPI_Modules_Complete': 1,
                                                    u'PPI_Module_Overall_Health': 1,
                                                    u'Physical_Measurements': 1,
                                                    u'Samples_Received': 1}}, u'hpo': u'UNSET'},
                       {u'date': u'2018-01-03',
                        u'metrics': {u'not_completed': {u'Full_Participant': 2,
                                                        u'PPI_Module_The_Basics': 1,
                                                        u'Consent_Complete': 1,
                                                        u'Consent_Enrollment': 0,
                                                        u'PPI_Module_Lifestyle': 1,
                                                        u'Registered': 0,
                                                        u'Baseline_PPI_Modules_Complete': 1,
                                                        u'PPI_Module_Overall_Health': 1,
                                                        u'Physical_Measurements': 1,
                                                        u'Samples_Received': 1},
                                     u'completed': {u'Full_Participant': 0,
                                                    u'PPI_Module_The_Basics': 1,
                                                    u'Consent_Complete': 1,
                                                    u'Consent_Enrollment': 2,
                                                    u'PPI_Module_Lifestyle': 1,
                                                    u'Registered': 2,
                                                    u'Baseline_PPI_Modules_Complete': 1,
                                                    u'PPI_Module_Overall_Health': 1,
                                                    u'Physical_Measurements': 1,
                                                    u'Samples_Received': 1}}, u'hpo': u'PITT'},
                       {u'date': u'2018-01-03',
                        u'metrics': {u'not_completed': {u'Full_Participant': 1,
                                                        u'PPI_Module_The_Basics': 1,
                                                        u'Consent_Complete': 0,
                                                        u'Consent_Enrollment': 0,
                                                        u'PPI_Module_Lifestyle': 1,
                                                        u'Registered': 0,
                                                        u'Baseline_PPI_Modules_Complete': 1,
                                                        u'PPI_Module_Overall_Health': 1,
                                                        u'Physical_Measurements': 1,
                                                        u'Samples_Received': 1},
                                     u'completed': {u'Full_Participant': 1,
                                                    u'PPI_Module_The_Basics': 1,
                                                    u'Consent_Complete': 2,
                                                    u'Consent_Enrollment': 2,
                                                    u'PPI_Module_Lifestyle': 1,
                                                    u'Registered': 2,
                                                    u'Baseline_PPI_Modules_Complete': 1,
                                                    u'PPI_Module_Overall_Health': 1,
                                                    u'Physical_Measurements': 1,
                                                    u'Samples_Received': 1}}, u'hpo': u'AZ_TUCSON'}
                       ])

    self.assertEquals(results2,
                      [{u'date': u'2018-01-08',
                        u'metrics': {u'not_completed': {u'Full_Participant': 0,
                                                        u'PPI_Module_The_Basics': 0,
                                                        u'Consent_Complete': 0,
                                                        u'Consent_Enrollment': 0,
                                                        u'PPI_Module_Lifestyle': 0,
                                                        u'Registered': 0,
                                                        u'Baseline_PPI_Modules_Complete': 0,
                                                        u'PPI_Module_Overall_Health': 0,
                                                        u'Physical_Measurements': 0,
                                                        u'Samples_Received': 0},
                                     u'completed': {u'Full_Participant': 2,
                                                    u'PPI_Module_The_Basics': 2,
                                                    u'Consent_Complete': 2,
                                                    u'Consent_Enrollment': 2,
                                                    u'PPI_Module_Lifestyle': 2,
                                                    u'Registered': 2,
                                                    u'Baseline_PPI_Modules_Complete': 2,
                                                    u'PPI_Module_Overall_Health': 2,
                                                    u'Physical_Measurements': 2,
                                                    u'Samples_Received': 2}}, u'hpo': u'PITT'},
                       {u'date': u'2018-01-08',
                        u'metrics': {u'not_completed': {u'Full_Participant': 0,
                                                        u'PPI_Module_The_Basics': 0,
                                                        u'Consent_Complete': 0,
                                                        u'Consent_Enrollment': 0,
                                                        u'PPI_Module_Lifestyle': 0,
                                                        u'Registered': 0,
                                                        u'Baseline_PPI_Modules_Complete': 0,
                                                        u'PPI_Module_Overall_Health': 0,
                                                        u'Physical_Measurements': 0,
                                                        u'Samples_Received': 0},
                                     u'completed': {u'Full_Participant': 2,
                                                    u'PPI_Module_The_Basics': 2,
                                                    u'Consent_Complete': 2,
                                                    u'Consent_Enrollment': 2,
                                                    u'PPI_Module_Lifestyle': 2,
                                                    u'Registered': 2,
                                                    u'Baseline_PPI_Modules_Complete': 2,
                                                    u'PPI_Module_Overall_Health': 2,
                                                    u'Physical_Measurements': 2,
                                                    u'Samples_Received': 2}}, u'hpo': u'AZ_TUCSON'}
                       ])

  def test_get_metrics_lifecycle_data_api_v2(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp=self.time3, time_fp_stored=self.time3)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time5, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time4, time_fp_stored=self.time5)
    p5 = Participant(participantId=7, biobankId=10)
    self._insert(p5, 'Chad4', 'Caterpillar4', 'PITT', time_int=self.time0, time_study=self.time0,
                 time_mem=self.time0, time_fp=self.time0, time_fp_stored=self.time0)

    p6 = Participant(participantId=8, biobankId=11)
    ppi_modules = dict(
      questionnaireOnTheBasicsTime=self.time0,
      questionnaireOnLifestyleTime=self.time0,
      questionnaireOnOverallHealthTime=self.time0,
      questionnaireOnHealthcareAccessTime=self.time0,
      questionnaireOnMedicalHistoryTime=self.time0,
      questionnaireOnMedicationsTime=self.time0,
      questionnaireOnFamilyHealthTime=self.time5
    )
    self._insert(p6, 'Chad5', 'Caterpillar5', 'PITT', time_int=self.time0, time_study=self.time0,
                 time_mem=self.time0, time_fp=self.time0, time_fp_stored=self.time0, **ppi_modules)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType
                                                                    .METRICS_V2_API))
    service.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType
                                                                    .PUBLIC_METRICS_EXPORT_API))

    qs1 = """
                          &stratification=LIFECYCLE
                          &endDate=2018-01-03
                          &history=TRUE
                          &version=2
                          """

    qs1 = ''.join(qs1.split())
    results = self.send_get('ParticipantCountsOverTime', query_string=qs1)
    self.assertEquals(results, [{u'date': u'2018-01-03',
                                 u'metrics':
                                   {u'not_completed':
                                      {u'Full_Participant': 0,
                                       u'PPI_Module_The_Basics': 0,
                                       u'Consent_Complete': 0,
                                       u'Consent_Enrollment': 0,
                                       u'PPI_Module_Lifestyle': 0,
                                       u'Registered': 0,
                                       u'Baseline_PPI_Modules_Complete': 0,
                                       u'Physical_Measurements': 0,
                                       u'PPI_Module_Family_Health': 0,
                                       u'PPI_Module_Overall_Health': 0,
                                       u'PPI_Module_Medical_History': 0,
                                       u'PPI_Retention_Modules_Complete': 0,
                                       u'PPI_Module_Healthcare_Access': 0,
                                       u'Samples_Received': 0},
                                    u'completed':
                                      {u'Full_Participant': 1,
                                       u'PPI_Module_The_Basics': 1,
                                       u'Consent_Complete': 1,
                                       u'Consent_Enrollment': 1,
                                       u'PPI_Module_Lifestyle': 1,
                                       u'Registered': 1,
                                       u'Baseline_PPI_Modules_Complete': 1,
                                       u'Physical_Measurements': 1,
                                       u'PPI_Module_Family_Health': 0,
                                       u'PPI_Module_Overall_Health': 1,
                                       u'PPI_Module_Medical_History': 0,
                                       u'PPI_Retention_Modules_Complete': 0,
                                       u'PPI_Module_Healthcare_Access': 0,
                                       u'Samples_Received': 1}}, u'hpo': u'UNSET'},
                                {u'date': u'2018-01-03',
                                 u'metrics':
                                   {u'not_completed':
                                      {u'Full_Participant': 2,
                                       u'PPI_Module_The_Basics': 1,
                                       u'Consent_Complete': 1,
                                       u'Consent_Enrollment': 0,
                                       u'PPI_Module_Lifestyle': 1,
                                       u'Registered': 0,
                                       u'Baseline_PPI_Modules_Complete': 1,
                                       u'Physical_Measurements': 1,
                                       u'PPI_Module_Family_Health': 1,
                                       u'PPI_Module_Overall_Health': 1,
                                       u'PPI_Module_Medical_History': 0,
                                       u'PPI_Retention_Modules_Complete': 1,
                                       u'PPI_Module_Healthcare_Access': 0,
                                       u'Samples_Received': 1},
                                    u'completed':
                                      {u'Full_Participant': 2,
                                       u'PPI_Module_The_Basics': 3,
                                       u'Consent_Complete': 3,
                                       u'Consent_Enrollment': 4,
                                       u'PPI_Module_Lifestyle': 3,
                                       u'Registered': 4,
                                       u'Baseline_PPI_Modules_Complete': 3,
                                       u'Physical_Measurements': 3,
                                       u'PPI_Module_Family_Health': 1,
                                       u'PPI_Module_Overall_Health': 3,
                                       u'PPI_Module_Medical_History': 2,
                                       u'PPI_Retention_Modules_Complete': 1,
                                       u'PPI_Module_Healthcare_Access': 2,
                                       u'Samples_Received': 3}}, u'hpo': u'PITT'},
                                {u'date': u'2018-01-03',
                                 u'metrics':
                                   {u'not_completed':
                                      {u'Full_Participant': 1,
                                       u'PPI_Module_The_Basics': 1,
                                       u'Consent_Complete': 0,
                                       u'Consent_Enrollment': 0,
                                       u'PPI_Module_Lifestyle': 1,
                                       u'Registered': 0,
                                       u'Baseline_PPI_Modules_Complete': 1,
                                       u'Physical_Measurements': 1,
                                       u'PPI_Module_Family_Health': 0,
                                       u'PPI_Module_Overall_Health': 1,
                                       u'PPI_Module_Medical_History': 0,
                                       u'PPI_Retention_Modules_Complete': 0,
                                       u'PPI_Module_Healthcare_Access': 0,
                                       u'Samples_Received': 1},
                                    u'completed':
                                      {u'Full_Participant': 1,
                                       u'PPI_Module_The_Basics': 1,
                                       u'Consent_Complete': 2,
                                       u'Consent_Enrollment': 2,
                                       u'PPI_Module_Lifestyle': 1,
                                       u'Registered': 2,
                                       u'Baseline_PPI_Modules_Complete': 1,
                                       u'Physical_Measurements': 1,
                                       u'PPI_Module_Family_Health': 0,
                                       u'PPI_Module_Overall_Health': 1,
                                       u'PPI_Module_Medical_History': 0,
                                       u'PPI_Retention_Modules_Complete': 0,
                                       u'PPI_Module_Healthcare_Access': 0,
                                       u'Samples_Received': 1}}, u'hpo': u'AZ_TUCSON'}])

  def test_refresh_metrics_lifecycle_cache_data_for_public_metrics_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_study=self.time2,
                 time_mem=self.time2, time_fp=self.time3, time_fp_stored=self.time3)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time5, time_fp=self.time5, time_fp_stored=self.time5)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_study=self.time4,
                 time_mem=self.time4, time_fp=self.time4, time_fp_stored=self.time5)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_study=self.time1,
                 time_mem=self.time1, time_fp=self.time1, time_fp_stored=self.time1)

    service = ParticipantCountsOverTimeService()
    service.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType
                                                                    .METRICS_V2_API))
    dao = MetricsLifecycleCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
    service.refresh_data_for_metrics_cache(dao)

    results = dao.get_latest_version_from_cache('2018-01-03')
    self.assertEquals(results, [{'date': '2018-01-03',
                                 'metrics': {
                                   'not_completed': {
                                     'Full_Participant': 3, 'PPI_Module_The_Basics': 2,
                                     'Consent_Complete': 1, 'Consent_Enrollment': 0,
                                     'PPI_Module_Lifestyle': 2, 'Baseline_PPI_Modules_Complete': 2,
                                     'PPI_Module_Family_Health': 2, 'PPI_Module_Overall_Health': 2,
                                     'PPI_Module_Medications': 2,  'Physical_Measurements': 2,
                                     'Registered': 0, 'PPI_Module_Medical_History': 2,
                                     'PPI_Module_Healthcare_Access': 2, 'Samples_Received': 2},
                                   'completed': {
                                     'Full_Participant': 2, 'PPI_Module_The_Basics': 3,
                                     'Consent_Complete': 4, 'Consent_Enrollment': 5,
                                     'PPI_Module_Lifestyle': 3, 'Baseline_PPI_Modules_Complete': 3,
                                     'PPI_Module_Family_Health': 3, 'PPI_Module_Overall_Health': 3,
                                     'PPI_Module_Medications': 3, 'Physical_Measurements': 3,
                                     'Registered': 5, 'PPI_Module_Medical_History': 3,
                                     'PPI_Module_Healthcare_Access': 3, 'Samples_Received': 3}
                                   }
                                 }])

    results2 = dao.get_latest_version_from_cache('2018-01-08')
    self.assertEquals(results2, [{'date': '2018-01-08',
                                  'metrics': {
                                    'not_completed': {
                                      'Full_Participant': 0, 'PPI_Module_The_Basics': 0,
                                      'Consent_Complete': 0, 'Consent_Enrollment': 0,
                                      'PPI_Module_Lifestyle': 0, 'Baseline_PPI_Modules_Complete': 0,
                                      'PPI_Module_Family_Health': 0, 'PPI_Module_Overall_Health': 0,
                                      'PPI_Module_Medications': 0, 'Physical_Measurements': 0,
                                      'Registered': 0, 'PPI_Module_Medical_History': 0,
                                      'PPI_Module_Healthcare_Access': 0, 'Samples_Received': 0},
                                    'completed': {
                                      'Full_Participant': 5, 'PPI_Module_The_Basics': 5,
                                      'Consent_Complete': 5, 'Consent_Enrollment': 5,
                                      'PPI_Module_Lifestyle': 5, 'Baseline_PPI_Modules_Complete': 5,
                                      'PPI_Module_Family_Health': 5, 'PPI_Module_Overall_Health': 5,
                                      'PPI_Module_Medications': 5, 'Physical_Measurements': 5,
                                      'Registered': 5, 'PPI_Module_Medical_History': 5,
                                      'PPI_Module_Healthcare_Access': 5, 'Samples_Received': 5}
                                    }
                                  }])

  def test_refresh_metrics_language_cache_data(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1,
                 primary_language='en')

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, primary_language='es')

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4, primary_language='en')

    p4 = Participant(participantId=5, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time2,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsLanguageCacheDao()
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2017-12-30', '2018-01-03')

    self.assertIn({'date': '2017-12-30',
                   'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}, 'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'EN': 0, 'UNSET': 1, 'ES': 0}, 'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2017-12-30',
                   'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}, 'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'EN': 1, 'UNSET': 1, 'ES': 0}, 'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'EN': 1, 'UNSET': 1, 'ES': 1}, 'hpo': u'AZ_TUCSON'}, results)

  def test_metrics_language_cache_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1,
                 primary_language='en')

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, primary_language='es')

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4, primary_language='en')

    p4 = Participant(participantId=5, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time2,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsLanguageCacheDao()
    service.refresh_data_for_metrics_cache(dao)

    # test API without awardee and enrollmentStatus parameters
    qs1 = """
              &stratification=LANGUAGE
              &startDate=2017-12-30
              &endDate=2018-01-05
              &history=TRUE
              """

    qs1 = ''.join(qs1.split())  # Remove all whitespace

    results1 = self.send_get('ParticipantCountsOverTime', query_string=qs1)

    self.assertIn({'date': '2017-12-30',
                   'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}, 'hpo': u'UNSET'}, results1)
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'EN': 0, 'UNSET': 1, 'ES': 0}, 'hpo': u'UNSET'}, results1)
    self.assertIn({'date': '2017-12-30',
                   'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}, 'hpo': u'AZ_TUCSON'}, results1)
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'EN': 1, 'UNSET': 1, 'ES': 0}, 'hpo': u'AZ_TUCSON'}, results1)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'EN': 1, 'UNSET': 1, 'ES': 1}, 'hpo': u'AZ_TUCSON'}, results1)

    # test API with awardee parameters
    qs2 = """
                  &stratification=LANGUAGE
                  &startDate=2017-12-30
                  &endDate=2018-01-05
                  &history=TRUE
                  &awardee=AZ_TUCSON
                  """

    qs2 = ''.join(qs2.split())  # Remove all whitespace

    results2 = self.send_get('ParticipantCountsOverTime', query_string=qs2)

    self.assertNotIn({'date': '2017-12-30',
                      'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}, 'hpo': u'UNSET'}, results2)
    self.assertNotIn({'date': '2017-12-31',
                      'metrics': {'EN': 0, 'UNSET': 1, 'ES': 0}, 'hpo': u'UNSET'}, results2)
    self.assertIn({'date': '2017-12-30',
                   'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}, 'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2017-12-31',
                   'metrics': {'EN': 1, 'UNSET': 1, 'ES': 0}, 'hpo': u'AZ_TUCSON'}, results2)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'EN': 1, 'UNSET': 1, 'ES': 1}, 'hpo': u'AZ_TUCSON'}, results2)

    # test API with enrollmentStatus parameters
    qs3 = """
                      &stratification=LANGUAGE
                      &startDate=2017-12-30
                      &endDate=2018-01-05
                      &history=TRUE
                      &enrollmentStatus=MEMBER,FULL_PARTICIPANT
                      """

    qs3 = ''.join(qs3.split())  # Remove all whitespace

    results3 = self.send_get('ParticipantCountsOverTime', query_string=qs3)

    self.assertNotIn({'date': '2017-12-31',
                      'metrics': {'EN': 0, 'UNSET': 1, 'ES': 0}, 'hpo': u'UNSET'}, results3)
    self.assertIn({u'date': u'2018-01-02',
                   u'metrics': {u'EN': 0, u'ES': 0, u'UNSET': 0}, u'hpo': u'UNSET'}, results3)
    self.assertIn({u'date': u'2018-01-01',
                   u'metrics': {u'EN': 0, u'ES': 0, u'UNSET': 1}, u'hpo': u'AZ_TUCSON'}, results3)
    self.assertIn({u'date': u'2018-01-02',
                   u'metrics': {u'EN': 1, u'ES': 0, u'UNSET': 1}, u'hpo': u'AZ_TUCSON'}, results3)

  def test_refresh_metrics_language_cache_data_for_public_metrics_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1,
                 primary_language='en')

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, primary_language='es')

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4, primary_language='en')

    p4 = Participant(participantId=5, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time2,
                 time_fp_stored=self.time4)

    service = ParticipantCountsOverTimeService()
    dao = MetricsLanguageCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2017-12-30', '2018-01-03')

    self.assertIn({'date': '2017-12-30', 'metrics': {'EN': 0, 'UNSET': 0, 'ES': 0}}, results)
    self.assertIn({'date': '2017-12-31', 'metrics': {'EN': 1, 'UNSET': 2, 'ES': 0}}, results)
    self.assertIn({'date': '2018-01-03', 'metrics': {'EN': 1, 'UNSET': 2, 'ES': 1}}, results)

  def create_demographics_questionnaire(self):
    """Uses the demographics test data questionnaire.  Returns the questionnaire id"""
    return self.create_questionnaire('questionnaire3.json')

  def post_demographics_questionnaire(self,
                                      participant_id,
                                      questionnaire_id,
                                      cabor_signature_string=False,
                                      time=TIME_1, **kwargs):
    """POSTs answers to the demographics questionnaire for the participant"""
    answers = {'code_answers': [],
               'string_answers': [],
               'date_answers': [('dateOfBirth', kwargs.get('dateOfBirth'))]}
    if cabor_signature_string:
      answers['string_answers'].append(('CABoRSignature', kwargs.get('CABoRSignature')))
    else:
      answers['uri_answers'] = [('CABoRSignature', kwargs.get('CABoRSignature'))]

    for link_id in self.code_link_ids:
      if link_id in kwargs:
        if link_id == 'race':
          for race_code in kwargs[link_id]:
            concept = Concept(PPI_SYSTEM, race_code)
            answers['code_answers'].append((link_id, concept))
        else:
          concept = Concept(PPI_SYSTEM, kwargs[link_id])
          answers['code_answers'].append((link_id, concept))

    for link_id in self.string_link_ids:
      code = kwargs.get(link_id)
      answers['string_answers'].append((link_id, code))

    response_data = make_questionnaire_response_json(participant_id, questionnaire_id, **answers)

    with FakeClock(time):
      url = 'Participant/%s/QuestionnaireResponse' % participant_id
      return self.send_post(url, request_data=response_data)

  def test_stratification_TOTAL(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(
      p1, 'Alice', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4)
    )

    qs = urllib.urlencode([
      ('stratification', 'TOTAL'),
      ('startDate', '2018-01-01'),
      ('endDate', '2018-01-05')
    ])

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    counts_by_date = {
      day['date']: day['metrics']['TOTAL']
      for day in response
    }

    self.assertEqual(counts_by_date['2018-01-01'], 0)
    self.assertEqual(counts_by_date['2018-01-02'], 1)
    self.assertEqual(counts_by_date['2018-01-03'], 1)
    self.assertEqual(counts_by_date['2018-01-04'], 1)
    self.assertEqual(counts_by_date['2018-01-05'], 1)

  def test_stratification_EHR_CONSENT(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(
      p1, 'Alice', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4)
    )

    qs = urllib.urlencode([
      ('stratification', 'EHR_CONSENT'),
      ('startDate', '2018-01-01'),
      ('endDate', '2018-01-05')
    ])

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    counts_by_date = {
      day['date']: day['metrics']['EHR_CONSENT']
      for day in response
    }

    self.assertEqual(counts_by_date['2018-01-01'], 0)
    self.assertEqual(counts_by_date['2018-01-02'], 0)
    self.assertEqual(counts_by_date['2018-01-03'], 1)
    self.assertEqual(counts_by_date['2018-01-04'], 1)
    self.assertEqual(counts_by_date['2018-01-05'], 1)


  def test_stratification_EHR_RATIO(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(
      p1, 'Alice', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4)
    )

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(
      p2, 'Bob', 'Builder', 'AZ_TUCSON',
      time_int=datetime.datetime(2018, 1, 4),
      time_mem=datetime.datetime(2018, 1, 5),
      time_fp=datetime.datetime(2018, 1, 6)
    )

    qs = urllib.urlencode([
      ('stratification', 'EHR_RATIO'),
      ('startDate', '2018-01-01'),
      ('endDate', '2018-01-06')
    ])

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertEqual(len(response), 6)

    ratios_by_date = {
      day['date']: day['metrics']['EHR_RATIO']
      for day in response
    }

    self.assertEqual(ratios_by_date['2018-01-01'], 0)
    self.assertEqual(ratios_by_date['2018-01-02'], 0/1.0)
    self.assertEqual(ratios_by_date['2018-01-03'], 1/1.0)
    self.assertEqual(ratios_by_date['2018-01-04'], 1/2.0)
    self.assertEqual(ratios_by_date['2018-01-05'], 2/2.0)
    self.assertEqual(ratios_by_date['2018-01-06'], 2/2.0)
