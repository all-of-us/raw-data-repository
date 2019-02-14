import datetime
import urllib

from clock import FakeClock
import httplib

from dao.participant_dao import ParticipantDao
from model.hpo import HPO
from dao.hpo_dao import HPODao
from model.code import Code, CodeType
from dao.code_dao import CodeDao
from model.calendar import Calendar
from dao.calendar_dao import CalendarDao
from dao.participant_summary_dao import ParticipantSummaryDao
from test.unit_test.unit_test_util import FlaskTestBase, make_questionnaire_response_json
from model.participant import Participant
from concepts import Concept
from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, OrganizationType, TEST_HPO_NAME, TEST_HPO_ID,\
  WithdrawalStatus, make_primary_provider_link_for_name
from dao.participant_counts_over_time_service import ParticipantCountsOverTimeService
from dao.metrics_cache_dao import MetricsEnrollmentStatusCacheDao, MetricsGenderCacheDao, \
  MetricsAgeCacheDao, MetricsRaceCacheDao, MetricsRegionCacheDao, MetricsLifecycleCacheDao
from code_constants import (PPI_SYSTEM, RACE_WHITE_CODE, RACE_HISPANIC_CODE, RACE_AIAN_CODE,
                            RACE_NONE_OF_THESE_CODE, PMI_SKIP_CODE, RACE_MENA_CODE)

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
              time_fp_stored=None, gender_id=None, dob=None, state_id=None):
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
    if dob:
      summary.dateOfBirth = dob
    else:
      summary.dateOfBirth = datetime.date(1978, 10, 10)
    if state_id:
      summary.stateId = state_id

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
        summary.physicalMeasurementsFinalizedTime = time_fp
        summary.physicalMeasurementsTime = time_fp
        summary.sampleOrderStatus1ED04Time = time_fp
        summary.sampleOrderStatus1SALTime = time_fp
        summary.sampleStatus1ED04Time = time_fp
        summary.sampleStatus1SALTime = time_fp

    self.ps_dao.insert(summary)

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
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4)

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

  def test_get_history_enrollment_status_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time1, time_mem=self.time3,
                 time_fp_stored=self.time4)

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

  def test_get_history_enrollment_status_api_filtered_by_awardee(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'PITT', time_int=self.time3, time_mem=self.time4,
                 time_fp_stored=self.time5)

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

  def test_refresh_metrics_gender_cache_data(self):

    code1 = Code(codeId=354, system="a", value="a", display=u"a", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=356, system="b", value="b", display=u"b", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=355, system="c", value="c", display=u"c", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_id=354)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_id=356)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_id=355)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, gender_id=355)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_id=355)

    service = ParticipantCountsOverTimeService()
    dao = MetricsGenderCacheDao()
    service.refresh_data_for_metrics_cache(dao)
    results = dao.get_latest_version_from_cache('2017-12-31', '2018-01-08')

    self.assertIn({'date': '2017-12-31',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 0}, 'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 0}, 'hpo': u'UNSET'}, results)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'}, results)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Prefer not to say': 0L, 'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'}, results)

  def test_get_history_gender_api(self):

    code1 = Code(codeId=354, system="a", value="a", display=u"a", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=356, system="b", value="b", display=u"b", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=355, system="c", value="c", display=u"c", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_id=354)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_id=356)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_id=355)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'AZ_TUCSON', time_int=self.time4, gender_id=355)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_id=355)

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
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0}, 'hpo': u'UNSET'},
                  response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0}, 'hpo': u'UNSET'},
                  response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 2L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'},
                  response)

  def test_get_history_gender_api_filtered_by_awardee(self):

    code1 = Code(codeId=354, system="a", value="a", display=u"a", topic=u"a",
                 codeType=CodeType.MODULE, mapped=True)
    code2 = Code(codeId=356, system="b", value="b", display=u"b", topic=u"b",
                 codeType=CodeType.MODULE, mapped=True)
    code3 = Code(codeId=355, system="c", value="c", display=u"c", topic=u"c",
                 codeType=CodeType.MODULE, mapped=True)

    self.code_dao.insert(code1)
    self.code_dao.insert(code2)
    self.code_dao.insert(code3)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, gender_id=354)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, gender_id=356)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, gender_id=355)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time4, gender_id=355)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, gender_id=355)

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
                                  'Prefer not to say': 0, 'UNSET': 0, 'Man': 0}, 'hpo': u'UNSET'},
                     response)
    self.assertNotIn({'date': '2018-01-01',
                      'metrics': {'Woman': 1L, 'PMI_Skip': 0, 'UNMAPPED': 0,
                                  'Other/Additional Options': 0, 'Transgender': 0, 'Non-Binary': 0,
                                  'Prefer not to say': 0, 'UNSET': 0, 'Man': 0}, 'hpo': u'UNSET'},
                     response)
    self.assertIn({'date': '2018-01-01',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 0L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 1L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'UNMAPPED': 0,
                               'Other/Additional Options': 0, 'Transgender': 1L, 'Non-Binary': 0,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 1L}, 'hpo': u'AZ_TUCSON'},
                  response)
    self.assertIn({'date': '2018-01-03',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 1,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0}, 'hpo': 'PITT'},
                  response)
    self.assertIn({'date': '2018-01-08',
                   'metrics': {'Woman': 0, 'PMI_Skip': 0, 'Other/Additional Options': 0,
                               'Non-Binary': 0, 'UNMAPPED': 0, 'Transgender': 1,
                               'Prefer not to say': 0, 'UNSET': 0, 'Man': 0}, 'hpo': 'PITT'},
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

  def test_get_history_total_api(self):

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', unconsented=True, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_mem=self.time4,
                 time_fp_stored=self.time5)

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
    self._insert(p3, 'Chad', 'Caterpillar', 'PITT', time_int=self.time3, time_mem=self.time4,
                 time_fp_stored=self.time5)

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
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_fp=self.time1,
                 time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_fp=self.time2,
                 time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=2)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_fp=self.time1,
                 time_fp_stored=self.time1, state_id=1)

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
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                              'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'},
                                 {'date': '2018-01-01',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results3, [{'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                              'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'PITT'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'FULL_CENSUS')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_CENSUS')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'FULL_CENSUS')
    self.assertEquals(results1, [{'date': '2017-12-31',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'}])
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'},
                                 {'date': '2018-01-01',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results3, [{'date': '2018-01-02',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0},
                                  'hpo': u'PITT'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])

    results1 = dao.get_latest_version_from_cache('2017-12-31', 'FULL_AWARDEE')
    results2 = dao.get_latest_version_from_cache('2018-01-01', 'FULL_AWARDEE')
    results3 = dao.get_latest_version_from_cache('2018-01-02', 'FULL_AWARDEE')
    self.assertEquals(results1, [{'date': '2017-12-31', 'count': 1L, 'hpo': u'UNSET'}])
    self.assertEquals(results2, [{'date': '2018-01-01', 'count': 1L, 'hpo': u'UNSET'},
                                 {'date': '2018-01-01', 'count': 1L, 'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results3, [{'date': '2018-01-02', 'count': 1L, 'hpo': u'UNSET'},
                                 {'date': '2018-01-02', 'count': 2L, 'hpo': u'PITT'},
                                 {'date': '2018-01-02', 'count': 2L, 'hpo': u'AZ_TUCSON'}])

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
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_fp=self.time1,
                 time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_fp=self.time2,
                 time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=2)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_fp=self.time1,
                 time_fp_stored=self.time1, state_id=1)

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
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                              'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'},
                                 {'date': '2018-01-01',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results3, [{'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 1L, 'GA': 0,
                                              'IN': 0, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'UNSET'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'PITT'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])

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
    self.assertEquals(results2, [{'date': '2018-01-01',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'},
                                 {'date': '2018-01-01',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results3, [{'date': '2018-01-02',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'UNSET'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0},
                                  'hpo': u'PITT'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])

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
    self.assertEquals(results2, [{'date': '2018-01-01', 'count': 1L, 'hpo': u'UNSET'},
                                 {'date': '2018-01-01', 'count': 1L, 'hpo': u'AZ_TUCSON'}])
    self.assertEquals(results3, [{'date': '2018-01-02', 'count': 1L, 'hpo': u'UNSET'},
                                 {'date': '2018-01-02', 'count': 2L, 'hpo': u'PITT'},
                                 {'date': '2018-01-02', 'count': 2L, 'hpo': u'AZ_TUCSON'}])

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
    self._insert(p1, 'Alice', 'Aardvark', 'UNSET', time_int=self.time1, time_fp=self.time1,
                 time_fp_stored=self.time1, state_id=1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', time_int=self.time2, time_fp=self.time2,
                 time_fp_stored=self.time2, state_id=2)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=3)

    p4 = Participant(participantId=4, biobankId=7)
    self._insert(p4, 'Chad2', 'Caterpillar2', 'PITT', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=2)

    p4 = Participant(participantId=6, biobankId=9)
    self._insert(p4, 'Chad3', 'Caterpillar3', 'PITT', time_int=self.time3, time_fp=self.time3,
                 time_fp_stored=self.time3, state_id=2)

    # ghost participant should be filtered out
    p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
    self._insert(p_ghost, 'Ghost', 'G', 'AZ_TUCSON', time_int=self.time1, time_fp=self.time1,
                 time_fp_stored=self.time1, state_id=1)

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
    self.assertEquals(results3, [{'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 0, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 2L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'PITT'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WA': 0, 'DE': 0, 'DC': 0, 'WI': 0, 'WV': 0, 'HI': 0,
                                              'FL': 0, 'WY': 0, 'NH': 0, 'NJ': 0, 'NM': 0, 'TX': 0,
                                              'LA': 0, 'AK': 0, 'NC': 0, 'ND': 0, 'NE': 0, 'TN': 0,
                                              'NY': 0, 'PA': 0, 'RI': 0, 'NV': 0, 'VA': 0, 'CO': 0,
                                              'CA': 1L, 'AL': 0, 'AR': 0, 'VT': 0, 'IL': 0, 'GA': 0,
                                              'IN': 1L, 'IA': 0, 'MA': 0, 'AZ': 0, 'ID': 0, 'CT': 0,
                                              'ME': 0, 'MD': 0, 'OK': 0, 'OH': 0, 'UT': 0, 'MO': 0,
                                              'MN': 0, 'MI': 0, 'KS': 0, 'MT': 0, 'MS': 0, 'SC': 0,
                                              'KY': 0, 'OR': 0, 'SD': 0}, 'hpo': u'AZ_TUCSON'}])

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
    self.assertEquals(results3, [{'date': '2018-01-02',
                                  'metrics': {'WEST': 0, 'NORTHEAST': 0, 'MIDWEST': 2L, 'SOUTH': 0},
                                  'hpo': u'PITT'},
                                 {'date': '2018-01-02',
                                  'metrics': {'WEST': 1L, 'NORTHEAST': 0, 'MIDWEST': 1L, 'SOUTH': 0},
                                  'hpo': u'AZ_TUCSON'}])

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
    self.assertEquals(results3, [{'date': '2018-01-02', 'count': 2L, 'hpo': u'PITT'},
                                 {'date': '2018-01-02', 'count': 2L, 'hpo': u'AZ_TUCSON'}])

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
    dao = MetricsLifecycleCacheDao()
    service.refresh_data_for_metrics_cache(dao)

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
