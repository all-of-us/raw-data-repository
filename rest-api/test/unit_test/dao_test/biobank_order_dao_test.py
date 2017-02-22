from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_dao import ParticipantDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier
from model.participant import Participant
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest


class BiobankOrderDaoTest(SqlTestBase):
  def setUp(self):
    super(BiobankOrderDaoTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.dao = BiobankOrderDao()

  def test_bad_participant(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(participantId=999))

  def test_store_with_identifier(self):
    order_id = 567
    self.dao.insert(BiobankOrder(
        biobankOrderId=order_id,
        participantId=self.participant.participantId,
        identifiers=[BiobankOrderIdentifier(system='rdr', value='firstid')]))
    self.assertIsNotNone(self.dao.get(order_id))

  def test_reject_used_identifier(self):
    self.dao.insert(BiobankOrder(
        biobankOrderId=1,
        participantId=self.participant.participantId,
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(
          biobankOrderId=2,
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
