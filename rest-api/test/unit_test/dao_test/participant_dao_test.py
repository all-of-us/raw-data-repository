import datetime
import test_data

from dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from participant_enums import UNSET_HPO_ID
from unit_test_util import SqlTestBase, PITT_HPO_ID
from clock import FakeClock
from werkzeug.exceptions import BadRequest

class ParticipantDaoTest(SqlTestBase):
  def setUp(self):
    super(ParticipantDaoTest, self).setUp()
    self.setup_data()
    self.dao = ParticipantDao()
    self.participant_summary_dao = ParticipantSummaryDao()
    self.participant_history_dao = ParticipantHistoryDao()

  def test_get_before_insert(self):
    self.assertIsNone(self.dao.get(1))
    self.assertIsNone(self.participant_summary_dao.get(1))
    self.assertIsNone(self.participant_history_dao.get([1, 1]))

  def test_insert(self):
    p = Participant(participantId=1, biobankId=2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.dao.insert(p)
    expected_participant = Participant(participantId=1, version=1, biobankId=2, lastModified=time,
                                       signUpTime=time, hpoId=UNSET_HPO_ID)
    self.assertEquals(expected_participant.asdict(), p.asdict())

    p2 = self.dao.get(1)
    self.assertEquals(p.asdict(), p2.asdict())

    # Creating a participant also creates a ParticipantSummary and a ParticipantHistory row
    ps = self.participant_summary_dao.get(1)
    expected_ps = ParticipantSummary(participantId=1, biobankId=2,
                                     signUpTime=time, hpoId=UNSET_HPO_ID,
                                     numBaselineSamplesArrived=0, numCompletedBaselinePPIModules=0)
    self.assertEquals(expected_ps.asdict(), ps.asdict())
    ph = self.participant_history_dao.get([1, 1])
    expected_ph = ParticipantHistory(participantId=1, version=1, biobankId=2, lastModified=time,
                                     signUpTime=time, hpoId=UNSET_HPO_ID)
    self.assertEquals(expected_ph.asdict(), ph.asdict())


  def test_update(self):
    p = Participant(participantId=1, biobankId=2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.dao.insert(p)

    p.providerLink = test_data.primary_provider_link('PITT')
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      self.dao.update(p)

    # lastModified, hpoId, version is updated on p after being passed in
    p2 = self.dao.get(1);
    expected_participant = Participant(participantId=1, version=2, biobankId=2, lastModified=time2,
                                       signUpTime=time, hpoId=PITT_HPO_ID,
                                       providerLink=p2.providerLink)
    self.assertEquals(expected_participant.asdict(), p2.asdict())
    self.assertEquals(p.asdict(), p2.asdict())

    # Updating the participant provider link also updates the HPO ID on the participant summary.
    ps = self.participant_summary_dao.get(1)
    expected_ps = ParticipantSummary(participantId=1, biobankId=2,
                                     signUpTime=time, hpoId=PITT_HPO_ID,
                                     numBaselineSamplesArrived=0, numCompletedBaselinePPIModules=0)
    self.assertEquals(expected_ps.asdict(), ps.asdict())

    expected_ph = ParticipantHistory(participantId=1, version=1, biobankId=2, lastModified=time,
                                     signUpTime=time, hpoId=UNSET_HPO_ID)
    # And updating the participant adds a new ParticipantHistory row.
    ph = self.participant_history_dao.get([1, 1])
    self.assertEquals(expected_ph.asdict(), ph.asdict())
    ph2 = self.participant_history_dao.get([1, 2])
    expected_ph2 = ParticipantHistory(participantId=1, version=2, biobankId=2, lastModified=time2,
                                      signUpTime=time, hpoId=PITT_HPO_ID,
                                      providerLink=p2.providerLink)
    self.assertEquals(expected_ph2.asdict(), ph2.asdict())

  def test_bad_hpo_insert(self):
    p = Participant(participantId=1, version=1, biobankId=2,
                    providerLink = test_data.primary_provider_link('FOO'))
    try:
      self.dao.insert(p)
      fail ("Should have failed")
    except BadRequest:
      pass

  def test_bad_hpo_update(self):
    p = Participant(participantId=1, biobankId=2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.dao.insert(p)

    p.providerLink = test_data.primary_provider_link('FOO')
    try:
      self.dao.update(p)
      fail("Should have failed")
    except BadRequest:
      pass
