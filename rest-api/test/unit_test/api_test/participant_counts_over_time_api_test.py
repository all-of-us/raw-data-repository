import datetime
from clock import FakeClock
import httplib

from dao.participant_dao import ParticipantDao
from model.hpo import HPO
from dao.hpo_dao import HPODao
from model.calendar import Calendar
from dao.calendar_dao import CalendarDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.participant_dao import make_primary_provider_link_for_name
from test.unit_test.unit_test_util import FlaskTestBase
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, OrganizationType, TEST_HPO_NAME, TEST_HPO_ID
from participant_enums import WithdrawalStatus

class ParticipantCountsOverTimeApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantCountsOverTimeApiTest, self).setUp(use_mysql=True)
    self.dao = ParticipantDao()
    self.ps_dao = ParticipantSummaryDao()
    self.ps = ParticipantSummary()
    self.calendar_dao = CalendarDao()
    self.hpo_dao = HPODao()

    # Needed by ParticipantCountsOverTimeApi
    self.hpo_dao.insert(HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName='Test',
                       organizationType=OrganizationType.UNSET))

    self.time1 = datetime.datetime(2017, 12, 31)
    self.time2 = datetime.datetime(2018, 1, 1)
    self.time3 = datetime.datetime(2018, 1, 2)
    self.time4 = datetime.datetime(2018, 1, 3)

    # Insert 2 weeks of dates
    curr_date = datetime.date(2017, 12, 22)
    for _ in xrange(0, 14):
      calendar_day = Calendar(day=curr_date )
      CalendarDao().insert(calendar_day)
      curr_date = curr_date + datetime.timedelta(days=1)

  def _insert(self, participant, first_name=None, last_name=None, hpo_name=None,
              unconsented=False, time_int=None, time_mem=None, time_fp=None):
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

    summary.dateOfBirth = datetime.date(1978, 10, 10)

    summary.enrollmentStatus = enrollment_status

    summary.hpoId = self.hpo_dao.get_by_name(hpo_name).hpoId

    if time_mem is not None:
      with FakeClock(time_mem):
        summary.consentForElectronicHealthRecordsTime = time_mem

    if time_fp is not None:
      with FakeClock(time_fp):
        summary.consentForElectronicHealthRecordsTime = time_fp
        summary.questionnaireOnTheBasicsTime = time_fp
        summary.questionnaireOnLifestyleTime = time_fp
        summary.questionnaireOnOverallHealthTime = time_fp
        summary.physicalMeasurementsFinalizedTime = time_fp
        summary.sampleOrderStatus1ED04Time = time_fp
        summary.sampleOrderStatus1SALTime = time_fp

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

  def test_get_counts_with_enrollment_status_full_participant_filter(self):

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
