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
    if obj.biobankOrderId is None:
      raise BadRequest('Client must supply biobankOrderId.')
    super(BiobankOrderDao, self)._validate_insert(session, obj)

  def insert_with_session(self, session, obj):
    if obj.logPosition is not None:
      raise BadRequest('%s.logPosition must be auto-generated.' % self.model_type.__name__)
    obj.logPosition = LogPosition()
    return super(BiobankOrderDao, self).insert_with_session(session, obj)

  def _validate_model(self, session, obj):
    if obj.participantId is None:
      raise BadRequest('participantId is required')
    ParticipantDao().validate_participant_reference(session, obj)
    if not participant_summary_dao.get_with_session(session, participant_id):

    for sample in obj.samples:
      self._validate_order_sample(sample)
    # TODO(mwf) FHIR validation for identifiers?
    # Verify that no identifier is in use by another order.
    for identifier in obj.identifiers:
      for existing in (session.query(BiobankOrderIdentifier)
          .filter_by(system=identifier.system)
          .filter_by(value=identifier.value)
          .filter(BiobankOrderIdentifier.biobankOrderId != obj.biobankOrderId)):
        raise BadRequest(
            'Identifier %s is already in use by order %s' % (identifier, existing.biobankOrderId))

  def _validate_order_sample(self, sample):
    # TODO(mwf) Make use of FHIR validation?
    if sample.test not in VALID_TESTS:
      raise BadRequest('Invalid test value %r not in %s.' % (sample.test, VALID_TESTS))

  def get_with_children(self, obj_id):
    with self.session() as session:
      return (session.query(BiobankOrder)
          .options(subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples))
          .get(obj_id))
