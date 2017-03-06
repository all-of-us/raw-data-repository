import clock
from dao.biobank_order_dao import BiobankOrderDao, VALID_TESTS
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest


class BiobankOrderDaoTest(SqlTestBase):
  _A_TEST = iter(VALID_TESTS).next()

  def setUp(self):
    super(BiobankOrderDaoTest, self).setUp()
    self.dao = BiobankOrderDao()

  def test_bad_participant(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(participantId=999))

  def test_reject_used_identifier(self):
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

  def test_store_invalid_test(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(
          biobankOrderId=2,
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')],
          samples=[BiobankOrderedSample(
              test='InvalidTestName', processingRequired=True, description=u'tested it')]))
