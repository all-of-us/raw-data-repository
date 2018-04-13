import datetime
from clock import FakeClock

from dao.participant_dao import ParticipantDao
from model.hpo import HPO
from dao.hpo_dao import HPODao
from model.calendar import Calendar
from dao.calendar_dao import CalendarDao
from dao.participant_summary_dao import ParticipantSummaryDao
from test.unit_test.unit_test_util import FlaskTestBase, random_ids
from model.participant import Participant
from participant_enums import OrganizationType, TEST_HPO_NAME, TEST_HPO_ID

class ParticipantCountsOverTimeApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantCountsOverTimeApiTest, self).setUp(use_mysql=True)
    self.dao = ParticipantDao()
    self.participant_summary_dao = ParticipantSummaryDao()
    self.calendar_dao = CalendarDao()
    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName='Test',
                     organizationType=OrganizationType.UNSET))

  def test_get_counts_with_default_parameters(self):
    p = Participant()

    time = datetime.datetime(2018, 1, 1)
    with random_ids([1, 2]):
      with FakeClock(time):
        self.dao.insert(p)

    curr_date = datetime.date(2017, 1, 1)
    # Insert 1 years of dates
    for _ in xrange(0, 365 * 1):
      calendar_day = Calendar(day=curr_date )
      CalendarDao().insert(calendar_day)
      curr_date = curr_date + datetime.timedelta(days=1)

    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2018-01-01
      &endDate=2018-01-16
      &awardee=
      &enrollmentStatus=
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertEquals(1, 1)
