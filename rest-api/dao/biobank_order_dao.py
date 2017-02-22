from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from model.biobank_order import BiobankOrder
from model.log_position import LogPosition

from werkzeug.exceptions import BadRequest


VALID_TESTS = frozenset(['1ED10', '2ED10', '1ED04', '1SST8', '1PST8', '1HEP4', '1UR10', '1SAL'])


class BiobankOrderDao(BaseDao):
  def __init__(self):
    super(BiobankOrderDao, self).__init__(BiobankOrder)

  def get_id(self, obj):
    return obj.biobankOrderId

  def _validate_insert(self, session, obj):
    super(BiobankOrderDao, self)._validate_insert(session, obj)
    if obj.biobankOrderId is None:
      raise BadRequest('Client must supply biobankOrderId.')
    if obj.logPositionId is not None:
      raise BadRequest('BiobankOrder LogPosition ID must be auto-generated.')

  def insert_with_session(self, session, obj):
    obj.logPosition = LogPosition()
    super(BiobankOrderDao, self).insert_with_session(session, obj)

  def _validate_model(self, session, obj):
    if obj.participantId is None:
      raise BadRequest('participantId is required')
    participant = ParticipantDao().get_with_session(session, obj.participantId)
    if not participant:
      raise BadRequest('%r does not reference a valid participant.' % obj.participantId)
    for sample in obj.samples:
      self._validate_order_sample(sample)
    # Verify that all order identifiers are not in use by another order
    if not obj.identifiers:
      raise BadRequest('At least one identifier is required.')
    for identifier in obj.identifiers:
      other_order = self._find_by_identifier(session, identifier)
      if other_order and other_order.biobankOrderIdentifier != obj.biobankOrderIdentifier:
        raise BadRequest(
            'Identifier %s is already in use by order %s'
            % (identifier, other_order.biobankOrderId))

  def _validate_order_sample(self, sample):
    if not sample.test:
      raise BadRequest('Missing field: sample.test in %s.' % sample)
    if not sample.description:
      raise BadRequest('Missing field: sample.description in %s.' % sample)
    if sample.test not in VALID_TESTS:
      raise BadRequest('Invalid test value %r not in %s.' % (sample.test, VALID_TESTS))

  def _find_by_identifier(self, session, identifier):
    return None
