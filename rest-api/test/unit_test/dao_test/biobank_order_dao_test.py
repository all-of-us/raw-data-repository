import clock
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.participant import Participant
from participant_enums import WithdrawalStatus
from dao.participant_dao import ParticipantDao
from test.test_data import load_biobank_order_json
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest, Forbidden, Conflict

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

  def test_from_json(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId)
    order = BiobankOrderDao().from_client_json(order_json, self.participant.participantId)
    self.assertEquals(1, order.sourceSiteId)
    self.assertEquals('fred@pmi-ops.org', order.sourceUsername)
    self.assertEquals(1, order.collectedSiteId)
    self.assertEquals('joe@pmi-ops.org', order.collectedUsername)
    self.assertEquals(1, order.processedSiteId)
    self.assertEquals('sue@pmi-ops.org', order.processedUsername)
    self.assertEquals(1, order.finalizedSiteId)
    self.assertEquals('bob@pmi-ops.org', order.finalizedUsername)

  def test_to_json(self):
    order = BiobankOrder(
        biobankOrderId='1',
        created=clock.CLOCK.now(),
        participantId=self.participant.participantId,
        sourceSiteId=1,
        sourceUsername='fred@pmi-ops.org',
        collectedSiteId=1,
        collectedUsername ='joe@pmi-ops.org',
        processedSiteId=1,
        processedUsername='sue@pmi-ops.org',
        finalizedSiteId=1,
        finalizedUsername='bob@pmi-ops.org',
        identifiers=[BiobankOrderIdentifier(system='a', value='c')],
        samples=[BiobankOrderedSample(biobankOrderId='1', test='blah', description='description',
                                      processingRequired=True)])
    order_json = BiobankOrderDao().to_client_json(order)
    expected_order_json = load_biobank_order_json(self.participant.participantId)
    for key in ['createdInfo', 'collectedInfo', 'processedInfo', 'finalizedInfo',
                'sourceSite', 'finalizedSite', 'author']:
      self.assertEquals(expected_order_json[key], order_json.get(key))

  def test_duplicate_insert_ok(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(BiobankOrder(
        biobankOrderId='1',
        participantId=self.participant.participantId,
        created=clock.CLOCK.now(),
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    order_2 = self.dao.insert(BiobankOrder(
        biobankOrderId='1',
        created=clock.CLOCK.now(),
        participantId=self.participant.participantId,
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    self.assertEquals(order_1.asdict(), order_2.asdict())

  def test_same_id_different_identifier_not_ok(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(BiobankOrder(
        biobankOrderId='1',
        participantId=self.participant.participantId,
        created=clock.CLOCK.now(),
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    with self.assertRaises(Conflict):
      self.dao.insert(BiobankOrder(
          biobankOrderId='1',
          created=clock.CLOCK.now(),
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='c')]))

  def test_reject_used_identifier(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(BiobankOrder(
        biobankOrderId='1',
        participantId=self.participant.participantId,
        created=clock.CLOCK.now(),
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankOrder(
          biobankOrderId='2',
          created=clock.CLOCK.now(),
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')]))

  def test_order_for_withdrawn_participant_fails(self):
    self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
    ParticipantDao().update(self.participant)
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    with self.assertRaises(Forbidden):
      self.dao.insert(BiobankOrder(
          biobankOrderId='1',
          participantId=self.participant.participantId,
          created=clock.CLOCK.now(),
          identifiers=[BiobankOrderIdentifier(system='a', value='b')]))

  def test_get_for_withdrawn_participant_fails(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(BiobankOrder(
        biobankOrderId='1',
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
          biobankOrderId='2',
          participantId=self.participant.participantId,
          identifiers=[BiobankOrderIdentifier(system='a', value='b')],
          samples=[BiobankOrderedSample(
              test='InvalidTestName', processingRequired=True, description=u'tested it')]))
