import clock
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.participant import Participant
from participant_enums import WithdrawalStatus, BiobankOrderStatus
from dao.participant_dao import ParticipantDao
from test.test_data import load_biobank_order_json
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest, Forbidden, Conflict

class BiobankOrderDaoTest(SqlTestBase):
  _A_TEST = BIOBANK_TESTS[0]

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
    order_json = BiobankOrderDao().to_client_json(order)
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
    self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

    self.assertEqual(order_1.version, 2)
    self.assertEqual(order_1.cancelledUsername, 'mike@pmi-ops.org')
    self.assertEqual(order_1.orderStatus, BiobankOrderStatus.CANCELLED)
    self.assertEqual(order_1.amendedReason, cancelled_request['amendedReason'])

  def test_cancelling_an_order_missing_reason(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['amendedReason']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

  def test_cancelling_an_order_missing_info(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())

    # missing cancelled info
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['cancelledInfo']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

    # missing site
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['cancelledInfo']['site']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

    # missing author
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['cancelledInfo']['author']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

    # missing status
    cancelled_request = self._get_cancel_patch()
    del cancelled_request['status']
    with self.assertRaises(BadRequest):
      self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

  def test_restoring_an_order(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    order_1 = self.dao.insert(self._make_biobank_order())
    cancelled_request = self._get_cancel_patch()
    self.dao.update_with_patch(order_1, cancelled_request, order_1.version)

    restore_request = self._get_restore_patch()
    self.dao.update_with_patch(order_1, restore_request, order_1.version)

    self.assertEqual(order_1.version, 3)
    self.assertEqual(order_1.restoredUsername, 'mike@pmi-ops.org')
    self.assertEqual(order_1.orderStatus, BiobankOrderStatus.UNSET)
    self.assertEqual(order_1.amendedReason, restore_request['amendedReason'])
