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

    curr_date = datetime.date(2017, 12, 22)
    # Insert 2 weeks of dates
    for _ in xrange(0, 14):
      calendar_day = Calendar(day=curr_date )
      CalendarDao().insert(calendar_day)
      curr_date = curr_date + datetime.timedelta(days=1)

  def _insert(self, participant, first_name=None, last_name=None, hpo_name=None,
              date_of_birth=None, time_reg=None, time_pair=None):

    with FakeClock(time_reg):
      self.dao.insert(participant)

    participant.providerLink = make_primary_provider_link_for_name(hpo_name)
    with FakeClock(time_pair):
      self.dao.update(participant)
    summary = self.participant_summary(participant)

    if first_name:
      summary.firstName = first_name
    if last_name:
      summary.lastName = last_name
    if date_of_birth:
      summary.dateOfBirth = date_of_birth
    summary.enrollmentStatus = EnrollmentStatus.MEMBER
    summary.hpoId = PITT_HPO_ID

    with FakeClock(time_pair):
      self.ps_dao.insert(summary)

    print('dict(self.dao.get(1))')
    ps = dict(self.ps_dao.get(1))
    for key in ps:
      value = str(ps[key])
      if value != 'None':
        print(key + ': ' + str(ps[key]))

    return participant

  def test_get_counts_with_default_parameters(self):

    time = datetime.datetime(2017, 12, 31)
    time2 = datetime.datetime(2018, 1, 1)

    date_of_birth = datetime.date(1978, 10, 10)

    p1 = Participant(participantId=1, biobankId=4)
    self._insert(p1, 'Alice', 'Aardvark', 'PITT', date_of_birth, time_reg=time, time_pair=time2)

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

    print('response')
    print(response)

    interested_count_day_1 = response[0]['metrics']['INTERESTED']
    interested_count_day_2 = response[1]['metrics']['INTERESTED']

    self.assertEquals(interested_count_day_1, 0)
    self.assertEquals(interested_count_day_2, 1)
