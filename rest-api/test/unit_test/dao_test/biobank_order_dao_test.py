import clock
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.participant import Participant
from participant_enums import WithdrawalStatus
from dao.participant_dao import ParticipantDao
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest, Forbidden

class BiobankOrderDaoTest(SqlTestBase):
  _A_TEST = iter(BIOBANK_TESTS).next()

  def setUp(self):
    super(BiobankOrderDaoTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.dao = BiobankOrderDao()

  def test_bad_participant(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(participantId=999))

  def test_reject_used_identifier(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(BiobankOrder(
        biobankOrderId=1,
        participantId=self.participant.participantId,
        created=clock.CLOCK.now(),
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(
          biobankOrderId=2,
          created=clock.CLOCK.now(),
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')]))

  def test_order_for_withdrawn_participant_fails(self):
    self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
    ParticipantDao().update(self.participant)
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    with self.assertRaises(Forbidden):
      self.dao.insert(BiobankOrder(
          biobankOrderId=1,
          participantId=self.participant.participantId,
          created=clock.CLOCK.now(),
          identifiers=[BiobankOrderIdentifier(system='a', value='b')]))

  def test_get_for_withdrawn_participant_fails(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(BiobankOrder(
        biobankOrderId=1,
        participantId=self.participant.participantId,
        created=clock.CLOCK.now(),
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
    ParticipantDao().update(self.participant)
    with self.assertRaises(Forbidden):
      self.dao.get(1)

  def test_store_invalid_test(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(
          biobankOrderId=2,
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')],
          samples=[BiobankOrderedSample(
              test='InvalidTestName', processingRequired=True, description=u'tested it')]))
