import clock
import json
from dao.base_dao import BaseDao, UpdatableDao
from dao.hpo_dao import HPODao
from model.participant_summary import ParticipantSummary
from model.participant import Participant, ParticipantHistory
from participant_enums import UNSET_HPO_ID, WithdrawalStatus, SuspensionStatus, EnrollmentStatus
from sqlalchemy.orm.session import make_transient
from werkzeug.exceptions import BadRequest

class ParticipantHistoryDao(BaseDao):
  """Maintains version history for participants.

  All previous versions of a participant are maintained (with the same participantId value and
  a new version value for each update.)

  Old versions of a participant are used to generate historical metrics (e.g. count the number of
  participants with different statuses or HPO IDs over time).

  Do not use this DAO for write operations directly; instead use ParticipantDao.
  """
  def __init__(self):
    super(ParticipantHistoryDao, self).__init__(ParticipantHistory)

  def get_id(self, obj):
    return [obj.participantId, obj.version]


class ParticipantDao(UpdatableDao):
  def __init__(self):
    super(ParticipantDao, self).__init__(Participant)

  def get_id(self, obj):
    return obj.participantId

  def insert_with_session(self, session, obj):
    obj.hpoId = self._get_hpo_id(obj)
    obj.version = 1
    obj.signUpTime = clock.CLOCK.now()
    obj.lastModified = obj.signUpTime
    if obj.withdrawalStatus is None:
      obj.withdrawalStatus = WithdrawalStatus.NOT_WITHDRAWN
    if obj.suspensionStatus is None:
      obj.suspensionStatus = SuspensionStatus.NOT_SUSPENDED
    super(ParticipantDao, self).insert_with_session(session, obj)
    history = ParticipantHistory()
    history.fromdict(obj.asdict(), allow_pk=True)
    session.add(history)
    return obj

  def insert(self, obj):
    if obj.participantId:
      assert obj.biobankId
      return super(ParticipantDao, self).insert(obj)
    assert not obj.biobankId
    return self._insert_with_random_id(obj, ('participantId', 'biobankId'))

  def _update_history(self, session, obj, existing_obj):
    # Increment the version and add a new history entry.
    obj.version = existing_obj.version + 1
    history = ParticipantHistory()
    history.fromdict(obj.asdict(), allow_pk=True)
    session.add(history)

  def _validate_update(self, session, obj, existing_obj):
    # Withdrawal and suspension have default values assigned on insert, so they should always have
    # explicit values in updates.
    if obj.withdrawalStatus is None:
      raise BadRequest('missing withdrawal status in update')
    if obj.suspensionStatus is None:
      raise BadRequest('missing suspension status in update')
    # Once a participant marks their withdrawal status as NO_USE, the participant can't be modified.
    check_not_withdrawn(existing_obj)
    super(ParticipantDao, self)._validate_update(session, obj, existing_obj)

  def _do_update(self, session, obj, existing_obj):
    """Updates the associated ParticipantSummary, and extracts HPO ID from the provider link."""
    obj.lastModified = clock.CLOCK.now()
    obj.signUpTime = existing_obj.signUpTime
    obj.biobankId = existing_obj.biobankId
    need_new_summary = False
    if obj.withdrawalStatus != existing_obj.withdrawalStatus:
      obj.withdrawalTime = (obj.lastModified if obj.withdrawalStatus == WithdrawalStatus.NO_USE
                            else None)
      need_new_summary = True
    if obj.suspensionStatus != existing_obj.suspensionStatus:
      obj.suspensionTime = (obj.lastModified if obj.suspensionStatus == SuspensionStatus.NO_CONTACT
                            else None)
      need_new_summary = True

    # If the provider link changes, update the HPO ID on the participant and its summary.
    obj.hpoId = existing_obj.hpoId
    if obj.providerLink != existing_obj.providerLink:
      new_hpo_id = self._get_hpo_id(obj)
      if new_hpo_id != existing_obj.hpoId:
        obj.hpoId = new_hpo_id
        need_new_summary = True

    if need_new_summary and existing_obj.participantSummary:
      # Copy the existing participant summary, and mutate the fields that
      # come from participant.
      summary = existing_obj.participantSummary
      summary.hpoId = obj.hpoId
      summary.withdrawalStatus = obj.withdrawalStatus
      summary.withdrawalTime = obj.withdrawalTime
      summary.suspensionStatus = obj.suspensionStatus
      summary.suspensionTime = obj.suspensionTime
      make_transient(summary)
      obj.participantSummary = summary
    self._update_history(session, obj, existing_obj)
    super(ParticipantDao, self)._do_update(session, obj, existing_obj)

  @staticmethod
  def create_summary_for_participant(obj):
    return ParticipantSummary(
        participantId=obj.participantId,
        biobankId=obj.biobankId,
        signUpTime=obj.signUpTime,
        hpoId=obj.hpoId,
        withdrawalStatus=obj.withdrawalStatus,
        suspensionStatus=obj.suspensionStatus,
        enrollmentStatus=EnrollmentStatus.INTERESTED)

  @staticmethod
  def _get_hpo_id(obj):
    hpo_name = get_HPO_name_from_participant(obj)
    if hpo_name:
      hpo = HPODao().get_by_name(hpo_name)
      if not hpo:
        raise BadRequest('No HPO found with name %s' % hpo_name)
      return hpo.hpoId
    else:
      return UNSET_HPO_ID

  def validate_participant_reference(self, session, obj):
    """Raises BadRequest if an object has a missing or invalid participantId reference,
    or if the participant has a withdrawal status of NO_USE."""
    if obj.participantId is None:
      raise BadRequest('%s.participantId required.' % obj.__class__.__name__)
    self.validate_participant_id(session, obj.participantId)

  def validate_participant_id(self, session, participant_id):
    """Raises BadRequest if a participant ID is invalid,
    or if the participant has a withdrawal status of NO_USE."""
    participant = self.get_with_session(session, participant_id)
    if participant is None:
      raise BadRequest(
          '%s.participantId %r is not found.' % (obj.__class__.__name__, participant_id))
    check_not_withdrawn(participant)
    return participant

  def get_valid_biobank_id_set(self, session):
    return set([row[0] for row in session.query(Participant.biobankId)])

  def get_biobank_ids_sample(self, session, percentage, batch_size):
    """Returns biobank ID and signUpTime for a percentage of participants.

    Used in generating fake biobank samples."""
    return (session.query(Participant.biobankId, Participant.signUpTime)
              .filter(Participant.biobankId % 100 <= percentage * 100)
              .yield_per(batch_size))

# TODO(danrodney): remove this logic from old participant code when done
def get_primary_provider_link(participant):
  if participant.providerLink:
    provider_links = json.loads(participant.providerLink)
    if provider_links:
      for provider in provider_links:
        if provider.get('primary') == True:
          return provider
  return None

def get_HPO_name_from_participant(participant):
  """Returns ExtractionResult with the string representing the HPO."""
  primary_provider_link = get_primary_provider_link(participant)
  if primary_provider_link and primary_provider_link.get('organization'):
    reference = primary_provider_link.get('organization').get('reference')
    if reference and reference.lower().startswith('organization/'):
      return reference[13:]
  return None

def check_not_withdrawn(obj):
  if obj.withdrawalStatus == WithdrawalStatus.NO_USE:
    raise BadRequest('Participant %d has withdrawn' % obj.participantId)
