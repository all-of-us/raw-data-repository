from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier
from model.log_position import LogPosition

from sqlalchemy.orm import subqueryload
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
    if not obj.identifiers:
      raise BadRequest('At least one identifier is required.')
    # Verify that all no identifier is in use by another order.
    for identifier in obj.identifiers:
      for existing in (session.query(BiobankOrderIdentifier)
          .filter_by(system=identifier.system)
          .filter_by(value=identifier.value)
          .filter(BiobankOrderIdentifier.biobankOrderId != obj.biobankOrderId)):
        raise BadRequest(
            'Identifier %s is already in use by order %d' % (identifier, existing.biobankOrderId))

  def _validate_order_sample(self, sample):
    if not sample.test:
      raise BadRequest('Missing field: sample.test in %s.' % sample)
    if not sample.description:
      raise BadRequest('Missing field: sample.description in %s.' % sample)
    if sample.test not in VALID_TESTS:
      raise BadRequest('Invalid test value %r not in %s.' % (sample.test, VALID_TESTS))

  def get_with_children(self, obj_id):
    with self.session() as session:
      return (session.query(BiobankOrder)
          .options(subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples))
          .get(obj_id))
