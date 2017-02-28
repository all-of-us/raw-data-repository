from dao.biobank_order_dao import BiobankOrderDao, VALID_TESTS
from dao.participant_dao import ParticipantDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.participant import Participant
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest


class BiobankOrderDaoTest(SqlTestBase):
  _A_TEST = iter(VALID_TESTS).next()

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
    fetched = self.dao.get_with_children(order_id)
    self.assertIsNotNone(fetched)
    self.assertEquals([('rdr', 'firstid')], [(i.system, i.value) for i in fetched.identifiers])

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

  def test_store_with_samples(self):
    order_id = 5
    self.dao.insert(BiobankOrder(
        biobankOrderId=order_id,
        participantId=self.participant.participantId,
        identifiers=[BiobankOrderIdentifier(system='a', value='b')],
        samples=[BiobankOrderedSample(
            test=self._A_TEST, processingRequired=True, description=u'tested \xe2')]))
    fetched = self.dao.get_with_children(order_id)
    self.assertEquals([self._A_TEST], [s.test for s in fetched.samples])
    self.assertEquals( u'tested \xe2', fetched.samples[0].description)

  def test_store_invalid_test(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(
          biobankOrderId=2,
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')],
          samples=[BiobankOrderedSample(
              test='InvalidTestName', processingRequired=True, description=u'tested it')]))
