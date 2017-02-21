from dao.base_dao import BaseDao
from model.biobank_order import BiobankOrder

from werkzeug.exceptions import BadRequest


VALID_TESTS = frozenset(['1ED10', '2ED10', '1ED04', '1SST8', '1PST8', '1HEP4', '1UR10', '1SAL'])


class BiobankOrderDao(BaseDao):
  def __init__(self):
    super(BiobankOrderDao, self).__init__(BiobankOrder)

  def get_id(self, obj):
    return obj.biobankOrderId

  def _validate_model(self, session, model):
    if not order.subject:
      raise BadRequest('Missing field: subject')
    if not order.created:
      raise BadRequest('Missing field: created')
    if not order.participantId:
      raise BadRequest('Missing participant ID')
    if order.subject != 'Patient/%d' % order.participantId:
      raise BadRequest(
          'Subject %r invalid / does not match participant ID %d.'
          % (order.subject, order.participantId)
    for sample in order.samples:
      self._validate_order_sample(sample)
    # Verify that all order identifiers are not in use by another order
    if not order.identifier or len(order.identifier) < 1:
      raise BadRequest('At least one identifier is required')
    for identifier in order.identifier:
      other_order = biobank_order.DAO().find_by_identifier(identifier)
      if other_order and other_order.id != order.id:
        raise BadRequest('Identifier {} is already in use by another order'.format(identifier))
    # This will raise if the participant can't be found.  Loading for validation only.
    participant_dao.DAO().load(a_id)

  def _validate_order_sample(self, sample)
    if not sample.test:
      raise BadRequest('Missing field: sample.test in %s.' % sample)
    if not sample.description:
      raise BadRequest('Missing field: sample.description in %s.' % sample)
    if sample.test not in VALID_TESTS:
      raise BadRequest('Invalid test value %r not in %s.' % (sample.test, VALID_TESTS))
