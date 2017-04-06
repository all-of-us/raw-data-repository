from code_constants import BIOBANK_TESTS_SET
from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao, check_not_withdrawn
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_order import BiobankOrder, BiobankOrderedSample, BiobankOrderIdentifier
from model.log_position import LogPosition
from model.participant import Participant

from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest

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
    participant_summary = ParticipantSummaryDao().get_with_session(session, obj.participantId)
    if not participant_summary:
      raise BadRequest("Can't submit order for participant %s without consent" % obj.participantId)
    check_not_withdrawn(participant_summary)
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
    if sample.test not in BIOBANK_TESTS_SET:
      raise BadRequest('Invalid test value %r not in %s.' % (sample.test, BIOBANK_TESTS_SET))

  def get_with_session(self, session, obj_id):
    result = super(BiobankOrderDao, self).get_with_session(session, obj_id)
    if result:
      ParticipantDao().validate_participant_reference(session, result)
    return result

  def get_with_children(self, obj_id):
    with self.session() as session:
      result = (session.query(BiobankOrder)
          .options(subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples))
          .get(obj_id))
      if result:
        ParticipantDao().validate_participant_reference(session, result)
      return result

  def get_ordered_samples_sample(self, session, percentage, batch_size):
    """Retrieves the biobank ID, collected time, and test for a percentage of ordered samples.
    Used in fake data generation."""
    return (session.query(Participant.biobankId, BiobankOrderedSample.collected,
                          BiobankOrderedSample.test)
                .join(BiobankOrder)
                .join(BiobankOrderedSample)
                .filter(Participant.biobankId % 100 < percentage * 100)
                .yield_per(batch_size))