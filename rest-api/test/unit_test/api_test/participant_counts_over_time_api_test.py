import datetime
from clock import FakeClock

from dao.participant_dao import ParticipantDao
from model.hpo import HPO
from dao.hpo_dao import HPODao
from model.calendar import Calendar
from dao.calendar_dao import CalendarDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.participant_dao import make_primary_provider_link_for_name
from test.unit_test.unit_test_util import FlaskTestBase, PITT_HPO_ID
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, OrganizationType, TEST_HPO_NAME, TEST_HPO_ID

class ParticipantCountsOverTimeApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantCountsOverTimeApiTest, self).setUp(use_mysql=True)
    self.dao = ParticipantDao()
    self.ps_dao = ParticipantSummaryDao()
    self.ps = ParticipantSummary()
    self.calendar_dao = CalendarDao()
    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName='Test',
                       organizationType=OrganizationType.UNSET))

    self.time1 = datetime.datetime(2017, 12, 31)
    self.time2 = datetime.datetime(2018, 1, 1)
    self.time3 = datetime.datetime(2018, 1, 2)
    self.time4 = datetime.datetime(2018, 1, 3)

    curr_date = datetime.date(2017, 12, 22)
    # Insert 2 weeks of dates
    for _ in xrange(0, 14):
      calendar_day = Calendar(day=curr_date )
      CalendarDao().insert(calendar_day)
      curr_date = curr_date + datetime.timedelta(days=1)

  def _insert(self, participant, first_name=None, last_name=None, hpo_name=None,
              date_of_birth=None, time_int=None, time_mem=None, time_fp=None):

    if time_mem == None:
      enrollment_status = EnrollmentStatus.INTERESTED
    elif time_fp:
      enrollment_status = EnrollmentStatus.MEMBER
    else:
      enrollment_status = EnrollmentStatus.FULL_PARTICIPANT


    with FakeClock(time_int):
      self.dao.insert(participant)

    participant.providerLink = make_primary_provider_link_for_name(hpo_name)
    with FakeClock(time_mem):
      self.dao.update(participant)
    summary = self.participant_summary(participant)

    if first_name:
      summary.firstName = first_name
    if last_name:
      summary.lastName = last_name
    if date_of_birth:
      summary.dateOfBirth = date_of_birth

    summary.enrollmentStatus = enrollment_status
    summary.hpoId = PITT_HPO_ID

    if time_mem == None:
      with FakeClock(time_mem):
        summary.consentForElectronicHealthRecordsTime = time_mem

    self.ps_dao.insert(summary)

    return participant

  def test_get_counts_with_default_parameters(self):
    # The most basic test in this class

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2017-12-30
      &endDate=2018-01-04
      &awardee=
      &enrollmentStatus=
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 1)

  def test_get_counts_with_single_awardee_filter(self):
    # Does the awardee filter work?

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    p1 = Participant(participantId=2, biobankId=5)
    self._insert(p1, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)

    p1 = Participant(participantId=3, biobankId=6)
    self._insert(p1, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)

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
      &enrollmentStatus=
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 2)

  def test_get_counts_with_single_awardee_filter(self):
    # Does the awardee filter work when passed a single awardee?

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    p1 = Participant(participantId=2, biobankId=5)
    self._insert(p1, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)

    p1 = Participant(participantId=3, biobankId=6)
    self._insert(p1, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)

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
        &enrollmentStatus=
        """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 2)

  def test_get_counts_with_multiple_awardee_filters(self):
    # Does the awardee filter work when passed more than one awardee?

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)

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

  def test_get_counts_with_single_various_filters(self):
    # Do the awardee and enrollment status filters work when passed multiple values?

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)

    p4 = Participant(participantId=4, biobankId=5)
    self._insert(p4, 'Debra', 'Dinosaur', 'PITT', dob, time_int=self.time1)

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

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 0)

  def test_get_counts_with_multiple_various_filters(self):
    # Do the awardee and enrollment status filters work when passed multiple values?

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)

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
    # Do the awardee and enrollment status filters work when passed multiple values?

    dob = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)

    p2 = Participant(participantId=2, biobankId=5)
    self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)

    p3 = Participant(participantId=3, biobankId=6)
    self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)

    qs = """
      bucketSize=1
      &stratification=TOTAL
      &startDate=2017-12-30
      &endDate=2018-01-04
      &awardee=
      &enrollmentStatus=
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    total_count_day_1 = response[0]['metrics']['TOTAL']
    total_count_day_2 = response[1]['metrics']['TOTAL']

    self.assertEquals(total_count_day_1, 0)
    self.assertEquals(total_count_day_2, 3)

  # def test_get_counts_with_total_stratification_filtered(self):
  #   # Do the awardee and enrollment status filters work when passed multiple values?
  #
  #   dob = datetime.date(1978, 10, 10)
  #
  #   p1 = Participant(participantId=1, biobankId=4)
  #   self._insert(p1, 'Alice', 'Aardvark', 'PITT', dob, time_int=self.time1)
  #
  #   p2 = Participant(participantId=2, biobankId=5)
  #   self._insert(p2, 'Bob', 'Builder', 'AZ_TUCSON', dob, time_int=self.time1)
  #
  #   p3 = Participant(participantId=3, biobankId=6)
  #   self._insert(p3, 'Chad', 'Caterpillar', 'AZ_TUCSON', dob, time_int=self.time1)
  #
  #   qs = """
  #     bucketSize=1
  #     &stratification=TOTAL
  #     &startDate=2017-12-30
  #     &endDate=2018-01-04
  #     &awardee=PITT
  #     &enrollmentStatus=INTERESTED
  #     """
  #
  #   qs = ''.join(qs.split())  # Remove all whitespace
  #
  #   response = self.send_get('ParticipantCountsOverTime', query_string=qs)
  #
  #   total_count_day_1 = response[0]['metrics']['TOTAL']
  #   total_count_day_2 = response[1]['metrics']['TOTAL']
  #
  #   self.assertEquals(total_count_day_1, 0)
  #   self.assertEquals(total_count_day_2, 1)
