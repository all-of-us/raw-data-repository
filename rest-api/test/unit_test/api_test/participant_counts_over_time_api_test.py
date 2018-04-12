import datetime
from clock import FakeClock

from dao.participant_dao import ParticipantDao
from dao.hpo_dao import HPODao
from model.hpo import HPO
from dao.participant_summary_dao import ParticipantSummaryDao
from test.unit_test.unit_test_util import FlaskTestBase, random_ids
from model.participant import Participant
from participant_enums import OrganizationType, TEST_HPO_NAME, TEST_HPO_ID

class ParticipantCountsOverTimeApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantCountsOverTimeApiTest, self).setUp(use_mysql=True)
    self.dao = ParticipantDao()
    self.participant_summary_dao = ParticipantSummaryDao()
    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName='Test',
                     organizationType=OrganizationType.UNSET))

  def test_get_counts_with_default_parameters(self):
    p = Participant()

    time = datetime.datetime(2016, 1, 1)
    with random_ids([1, 2]):
      with FakeClock(time):
        self.dao.insert(p)

    qs = """
      bucketSize=1
      &stratification=ENROLLMENT_STATUS
      &startDate=2018-01-01
      &endDate=2018-01-16
      &awardee=
      &enrollmentStatus=
      """

    qs = ''.join(qs.split())  # Remove all whitespace

    response = self.send_get('ParticipantCountsOverTime', query_string=qs)

    self.assertEquals(1, 1)
