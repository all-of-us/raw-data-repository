import datetime
import test_data

from dao.base_dao import MAX_INSERT_ATTEMPTS
from dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant import Participant
from unit_test_util import SqlTestBase, PITT_HPO_ID, random_ids
from clock import FakeClock
from werkzeug.exceptions import BadRequest, NotFound, PreconditionFailed, ServiceUnavailable


class ParticipantDaoTest(SqlTestBase):
  def setUp(self):
    super(ParticipantDaoTest, self).setUp()
    self.dao = ParticipantDao()
    self.participant_summary_dao = ParticipantSummaryDao()
    self.participant_history_dao = ParticipantHistoryDao()

  def test_get_before_insert(self):
    self.assertIsNone(self.dao.get(1))
    self.assertIsNone(self.participant_summary_dao.get(1))
    self.assertIsNone(self.participant_history_dao.get([1, 1]))

  def test_insert(self):
    p = Participant()
    time = datetime.datetime(2016, 1, 1)
    with random_ids([1, 2]):
      with FakeClock(time):
        self.dao.insert(p)
    expected_participant = self._participant_with_defaults(
        participantId=1, version=1, biobankId=2, lastModified=time, signUpTime=time)
    self.assertEquals(expected_participant.asdict(), p.asdict())

    p2 = self.dao.get(1)
    self.assertEquals(p.asdict(), p2.asdict())

    # Creating a participant also creates a ParticipantSummary and a ParticipantHistory row
    ps = self.participant_summary_dao.get(1)
    expected_ps = self._participant_summary_with_defaults(
        participantId=1, biobankId=2, signUpTime=time)
    self.assertEquals(expected_ps.asdict(), ps.asdict())
    ph = self.participant_history_dao.get([1, 1])
    expected_ph = self._participant_history_with_defaults(
        participantId=1, biobankId=2, lastModified=time, signUpTime=time)
    self.assertEquals(expected_ph.asdict(), ph.asdict())

  def test_insert_duplicate_participant_id_retry(self):
    p = Participant()
    with random_ids([1, 2]):
      self.dao.insert(p)
    p2 = Participant()
    time = datetime.datetime(2016, 1, 1)
    with random_ids([1, 3, 2, 3]):
      with FakeClock(time):
        p2 = self.dao.insert(p2)
    expected_participant = self._participant_with_defaults(
        participantId=2, version=1, biobankId=3, lastModified=time, signUpTime=time)
    self.assertEquals(expected_participant.asdict(), p2.asdict())

  def test_insert_duplicate_participant_id_give_up(self):
    p = Participant()
    with random_ids([1, 2]):
      self.dao.insert(p)
    rand_ints = []
    for i in range(0, MAX_INSERT_ATTEMPTS):
      rand_ints.append(1)
      rand_ints.append(i)
    p2 = Participant()
    with random_ids(rand_ints):
      with self.assertRaises(ServiceUnavailable):
        self.dao.insert(p2)

  def test_insert_duplicate_biobank_id_give_up(self):
    p = Participant()
    with random_ids([1, 2]):
      self.dao.insert(p)
    rand_ints = []
    for i in range(0, MAX_INSERT_ATTEMPTS):
      rand_ints.append(i + 2)
      rand_ints.append(2)
    p2 = Participant()
    with random_ids(rand_ints):
      with self.assertRaises(ServiceUnavailable):
        self.dao.insert(p2)

  def test_update_no_expected_version(self):
    p = Participant()
    time = datetime.datetime(2016, 1, 1)
    with random_ids([1, 2]):
      with FakeClock(time):
        self.dao.insert(p)

    p.providerLink = test_data.primary_provider_link('PITT')
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      self.dao.update(p)

    # lastModified, hpoId, version is updated on p after being passed in
    p2 = self.dao.get(1);
    expected_participant = self._participant_with_defaults(
        participantId=1, version=2, biobankId=2, lastModified=time2, signUpTime=time,
        hpoId=PITT_HPO_ID, providerLink=p2.providerLink)
    self.assertEquals(expected_participant.asdict(), p2.asdict())
    self.assertEquals(p.asdict(), p2.asdict())

    # Updating the participant provider link also updates the HPO ID on the participant summary.
    ps = self.participant_summary_dao.get(1)
    expected_ps = self._participant_summary_with_defaults(
        participantId=1, biobankId=2, signUpTime=time, hpoId=PITT_HPO_ID)
    self.assertEquals(expected_ps.asdict(), ps.asdict())

    expected_ph = self._participant_history_with_defaults(
        participantId=1, biobankId=2, lastModified=time, signUpTime=time)
    # And updating the participant adds a new ParticipantHistory row.
    ph = self.participant_history_dao.get([1, 1])
    self.assertEquals(expected_ph.asdict(), ph.asdict())
    ph2 = self.participant_history_dao.get([1, 2])
    expected_ph2 = self._participant_history_with_defaults(
        participantId=1, version=2, biobankId=2, lastModified=time2, signUpTime=time,
        hpoId=PITT_HPO_ID, providerLink=p2.providerLink)
    self.assertEquals(expected_ph2.asdict(), ph2.asdict())

  def test_update_right_expected_version(self):
    p = Participant()
    time = datetime.datetime(2016, 1, 1)
    with random_ids([1, 2]):
      with FakeClock(time):
        self.dao.insert(p)

    p.version = 1
    p.providerLink = test_data.primary_provider_link('PITT')
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      self.dao.update(p)

    p2 = self.dao.get(1);
    expected_participant = self._participant_with_defaults(
        participantId=1, version=2, biobankId=2, lastModified=time2, signUpTime=time,
        hpoId=PITT_HPO_ID, providerLink=p2.providerLink)
    self.assertEquals(expected_participant.asdict(), p2.asdict())

  def test_update_wrong_expected_version(self):
    p = Participant()
    time = datetime.datetime(2016, 1, 1)
    with random_ids([1, 2]):
      with FakeClock(time):
        self.dao.insert(p)

    p.version = 2
    p.providerLink = test_data.primary_provider_link('PITT')
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      with self.assertRaises(PreconditionFailed):
        self.dao.update(p)

  def test_update_not_exists(self):
    p = self._participant_with_defaults(participantId=1, biobankId=2)
    with self.assertRaises(NotFound):
      self.dao.update(p)

  def test_bad_hpo_insert(self):
    p = Participant(participantId=1, version=1, biobankId=2,
                    providerLink = test_data.primary_provider_link('FOO'))
    with self.assertRaises(BadRequest):
      self.dao.insert(p)

  def test_bad_hpo_update(self):
    p = Participant(participantId=1, biobankId=2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.dao.insert(p)

    p.providerLink = test_data.primary_provider_link('FOO')
    with self.assertRaises(BadRequest):
      self.dao.update(p)
