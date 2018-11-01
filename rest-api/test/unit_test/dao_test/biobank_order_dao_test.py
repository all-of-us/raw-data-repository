import datetime
import clock
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.participant import Participant
from participant_enums import WithdrawalStatus, BiobankOrderStatus, OrderStatus
from dao.participant_dao import ParticipantDao
from test.test_data import load_biobank_order_json
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest, Forbidden, Conflict


class BiobankOrderDaoTest(SqlTestBase):
  _A_TEST = BIOBANK_TESTS[0]
  _B_TEST = BIOBANK_TESTS[1]
  TIME_1 = datetime.datetime(2018, 9, 20, 5, 49, 11)
  TIME_2 = datetime.datetime(2018, 9, 21, 8, 49, 37)

  def setUp(self):
    super(BiobankOrderDaoTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.dao = BiobankOrderDao()

  def _make_biobank_order(self, **kwargs):
    """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
    for k, default_value in (
        ('biobankOrderId', '1'),
        ('created', clock.CLOCK.now()),
        ('participantId', self.participant.participantId),
        ('sourceSiteId', 1),
        ('sourceUsername', 'fred@pmi-ops.org'),
        ('collectedSiteId', 1),
        ('collectedUsername', 'joe@pmi-ops.org'),
        ('processedSiteId', 1),
        ('processedUsername', 'sue@pmi-ops.org'),
        ('finalizedSiteId', 2),
        ('finalizedUsername', 'bob@pmi-ops.org'),
        ('identifiers', [BiobankOrderIdentifier(system='a', value='c')]),
        ('samples', [BiobankOrderedSample(
            biobankOrderId='1',
            test=self._A_TEST,
            description='description',
            processingRequired=True)])):
      if k not in kwargs:
        kwargs[k] = default_value
    return BiobankOrder(**kwargs)

  @staticmethod
  def _get_cancel_patch():
    return {
      "amendedReason": 'I messed something up :( ',
      "cancelledInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "mike@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      },
      "status": "cancelled"
    }

  @staticmethod
  def _get_restore_patch():
    return {
      "amendedReason": 'I didn"t mess something up :( ',
      "restoredInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "mike@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      },
      "status": "restored"
    }

  @staticmethod
  def _get_amended_info(order):
    amendment = dict(amendedReason='I had to change something', amendedInfo={
      "author": {
        "system": "https://www.pmi-ops.org/healthpro-username",
        "value": "mike@pmi-ops.org"
      },
      "site": {
        "system": "https://www.pmi-ops.org/site-id",
        "value": "hpo-site-monroeville"
      }
    })

    order.amendedReason = amendment['amendedReason']
    order.amendedInfo = amendment['amendedInfo']
    return order

  def test_bad_participant(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(self._make_biobank_order(participantId=999))

  def test_from_json(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId)
    order = BiobankOrderDao().from_client_json(order_json,
                                               participant_id=self.participant.participantId)
    self.assertEquals(1, order.sourceSiteId)
    self.assertEquals('fred@pmi-ops.org', order.sourceUsername)
    self.assertEquals(1, order.collectedSiteId)
    self.assertEquals('joe@pmi-ops.org', order.collectedUsername)
    self.assertEquals(1, order.processedSiteId)
    self.assertEquals('sue@pmi-ops.org', order.processedUsername)
    self.assertEquals(2, order.finalizedSiteId)
    self.assertEquals('bob@pmi-ops.org', order.finalizedUsername)

  def test_to_json(self):
    order = self._make_biobank_order()
    order_json = self.dao.to_client_json(order)
    expected_order_json = load_biobank_order_json(self.participant.participantId)
    for key in ('createdInfo', 'collectedInfo', 'processedInfo', 'finalizedInfo'):
      self.assertEquals(expected_order_json[key], order_json.get(key))

  def test_duplicate_insert_ok(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    order_2 = self.dao.insert(self._make_biobank_order())
    self.assertEquals(order_1.asdict(), order_2.asdict())

  def test_same_id_different_identifier_not_ok(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(self._make_biobank_order(
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    with self.assertRaises(Conflict):
      self.dao.insert(self._make_biobank_order(
          identifiers=[BiobankOrderIdentifier(system='a', value='c')]))

  def test_reject_used_identifier(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(self._make_biobank_order(
        biobankOrderId='1',
        identifiers=[BiobankOrderIdentifier(system='a', value='b')]))
    with self.assertRaises(BadRequest):
      self.dao.insert(self._make_biobank_order(
          biobankOrderId='2',
          identifiers=[BiobankOrderIdentifier(system='a', value='b')]))

  def test_order_for_withdrawn_participant_fails(self):
    self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
    ParticipantDao().update(self.participant)
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    with self.assertRaises(Forbidden):
      self.dao.insert(self._make_biobank_order(participantId=self.participant.participantId))

  def test_get_for_withdrawn_participant_fails(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.dao.insert(self._make_biobank_order(
        biobankOrderId='1',
        participantId=self.participant.participantId))
    self.participant.version += 1
    self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
    ParticipantDao().update(self.participant)
    with self.assertRaises(Forbidden):
      self.dao.get(1)

  def test_store_invalid_test(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(self._make_biobank_order(
          samples=[BiobankOrderedSample(
              test='InvalidTestName', processingRequired=True, description=u'tested it')]))

  def test_cancelling_an_order(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    cancelled_request = self._get_cancel_patch()
    updated_order = self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request,
                                               order_1.version)

    self.assertEqual(updated_order.version, 2)
    self.assertEqual(updated_order.cancelledUsername, 'mike@pmi-ops.org')
    self.assertEqual(updated_order.orderStatus, BiobankOrderStatus.CANCELLED)
    self.assertEqual(updated_order.amendedReason, cancelled_request['amendedReason'])

  def test_cancelled_order_removes_from_participant_summary(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    samples = [BiobankOrderedSample(
      test=self._B_TEST, processingRequired=True, description=u'new sample')]
    biobank_order_id = 2
    with clock.FakeClock(self.TIME_1):
      order_1 = self.dao.insert(self._make_biobank_order())

    with clock.FakeClock(self.TIME_2):
      self.dao.insert(self._make_biobank_order(samples=samples,
                                               biobankOrderId=biobank_order_id,
                                               identifiers=[
                                                           BiobankOrderIdentifier(system='z',
                                                                                  value='x')]))
    cancelled_request = self._get_cancel_patch()
    ps_dao = ParticipantSummaryDao().get(self.participant.participantId)

    self.assertEqual(ps_dao.sampleOrderStatus1ED10, OrderStatus.CREATED)
    self.assertEqual(ps_dao.sampleOrderStatus1ED10Time, self.TIME_1)
    self.assertEqual(ps_dao.sampleOrderStatus2ED10, OrderStatus.CREATED)
    self.assertEqual(ps_dao.sampleOrderStatus2ED10Time, self.TIME_2)

    self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request,
                               order_1.version)
    ps_dao = ParticipantSummaryDao().get(self.participant.participantId)

    self.assertEqual(ps_dao.sampleOrderStatus1ED10, None)
    self.assertEqual(ps_dao.sampleOrderStatus1ED10Time, None)
    # should not remove the other order
    self.assertEqual(ps_dao.sampleOrderStatus2ED10, OrderStatus.CREATED)
    self.assertEqual(ps_dao.sampleOrderStatus2ED10Time, self.TIME_2)
    self.assertEqual(ps_dao.biospecimenCollectedSiteId, 1)
    self.assertEqual(ps_dao.biospecimenFinalizedSiteId, 2)
    self.assertEqual(ps_dao.biospecimenProcessedSiteId, 1)
    self.assertEqual(ps_dao.biospecimenStatus, OrderStatus.FINALIZED)

  def test_restoring_an_order_gets_to_participant_summary(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    cancelled_request = self._get_cancel_patch()
    cancelled_order = self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request,
                                                 order_1.version)

    restore_request = self._get_restore_patch()
    self.dao.update_with_patch(order_1.biobankOrderId, restore_request,
                                                cancelled_order.version)
    ps_dao = ParticipantSummaryDao().get(self.participant.participantId)
    self.assertEqual(ps_dao.sampleOrderStatus1ED10, OrderStatus.CREATED)

  def test_amending_order_participant_summary(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    amended_info = self._get_amended_info(order_1)
    amended_info.sourceSiteId = 2
    samples = [BiobankOrderedSample(
      test=self._B_TEST, processingRequired=True, description=u'new sample')]
    amended_info.samples = samples
    with self.dao.session() as session:
      self.dao._do_update(session, order_1, amended_info)

    amended_order = self.dao.get(1)
    self.assertEqual(amended_order.version, 2)

    ps_dao = ParticipantSummaryDao().get(self.participant.participantId)
    self.assertEqual(ps_dao.sampleOrderStatus2ED10, OrderStatus.CREATED)
    self.assertEqual(ps_dao.sampleOrderStatus1ED10, OrderStatus.CREATED)

  def test_cancelling_an_order_missing_reason(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['amendedReason']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request, order_1.version)

  def test_cancelling_an_order_missing_info(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())

    # missing cancelled info
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['cancelledInfo']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request, order_1.version)

    # missing site
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['cancelledInfo']['site']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request, order_1.version)

    # missing author
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['cancelledInfo']['author']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request, order_1.version)

    # missing status
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['status']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request, order_1.version)

  def test_restoring_an_order(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    cancelled_request = self._get_cancel_patch()
    cancelled_order = self.dao.update_with_patch(order_1.biobankOrderId, cancelled_request,
                                          order_1.version)

    restore_request = self._get_restore_patch()
    restored_order = self.dao.update_with_patch(order_1.biobankOrderId, restore_request,
                                    cancelled_order.version)

    self.assertEqual(restored_order.version, 3)
    self.assertEqual(restored_order.restoredUsername, 'mike@pmi-ops.org')
    self.assertEqual(restored_order.orderStatus, BiobankOrderStatus.UNSET)
    self.assertEqual(restored_order.amendedReason, restore_request['amendedReason'])

  def test_amending_an_order(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    amended_info = self._get_amended_info(order_1)
    amended_info.sourceSiteId = 2
    with self.dao.session() as session:
      self.dao._do_update(session, order_1, amended_info)

    amended_order = self.dao.get(1)
    self.assertEqual(amended_order.version, 2)
    self.assertEqual(amended_order.orderStatus, BiobankOrderStatus.AMENDED)
    self.assertEqual(amended_order.amendedReason, 'I had to change something')
