import json

from sqlalchemy.orm.session import make_transient
from sqlalchemy.orm import joinedload
from werkzeug.exceptions import BadRequest, Forbidden

from api_util import format_json_enum, parse_json_enum, format_json_date
import clock
from dao.base_dao import BaseDao, UpdatableDao
from dao.hpo_dao import HPODao
from model.participant_summary import ParticipantSummary
from model.participant import Participant, ParticipantHistory
from model.utils import to_client_participant_id, to_client_biobank_id
from participant_enums import UNSET_HPO_ID, WithdrawalStatus, SuspensionStatus, EnrollmentStatus


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
    super(ParticipantDao, self)._validate_update(session, obj, existing_obj)
    # Once a participant marks their withdrawal status as NO_USE, it can't be changed back.
    if (existing_obj.withdrawalStatus == WithdrawalStatus.NO_USE
        and obj.withdrawalStatus != WithdrawalStatus.NO_USE):
      raise Forbidden('Participant %d has withdrawn, cannot unwithdraw' % obj.participantId)

  def get_for_update(self, session, obj_id):
    # Fetch the participant summary at the same time as the participant, as we are potentially
    # updating both.
    return self.get_with_session(session, obj_id, for_update=True,
                                 options=joinedload(Participant.participantSummary))

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
      make_transient(obj)
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
    hpo_name = _get_hpo_name_from_participant(obj)
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
    return self.validate_participant_id(session, obj.participantId)

  def validate_participant_id(self, session, participant_id):
    """Raises BadRequest if a participant ID is invalid,
    or if the participant has a withdrawal status of NO_USE."""
    participant = self.get_with_session(session, participant_id)
    if participant is None:
      raise BadRequest('Participant with ID %d is not found.' % participant_id)
    raise_if_withdrawn(participant)
    return participant

  def get_biobank_ids_sample(self, session, percentage, batch_size):
    """Returns biobank ID and signUpTime for a percentage of participants.

    Used in generating fake biobank samples."""
    return (session.query(Participant.biobankId, Participant.signUpTime)
              .filter(Participant.biobankId % 100 <= percentage * 100)
              .yield_per(batch_size))

  def to_client_json(self, model):
    client_json = {
        'participantId': to_client_participant_id(model.participantId),
        'biobankId': to_client_biobank_id(model.biobankId),
        'lastModified': model.lastModified.isoformat(),
        'signUpTime': model.signUpTime.isoformat(),
        'providerLink': json.loads(model.providerLink),
        'withdrawalStatus': model.withdrawalStatus,
        'withdrawalTime': model.withdrawalTime,
        'suspensionStatus': model.suspensionStatus,
        'suspensionTime': model.suspensionTime
    }
    format_json_enum(client_json, 'withdrawalStatus')
    format_json_enum(client_json, 'suspensionStatus')
    format_json_date(client_json, 'withdrawalTime')
    format_json_date(client_json, 'suspensionTime')
    return client_json

  def from_client_json(self, resource_json, id_=None, expected_version=None, client_id=None):
    parse_json_enum(resource_json, 'withdrawalStatus', WithdrawalStatus)
    parse_json_enum(resource_json, 'suspensionStatus', SuspensionStatus)
    # biobankId, lastModified, signUpTime are set by DAO.
    return Participant(
        participantId=id_,
        version=expected_version,
        providerLink=json.dumps(resource_json.get('providerLink')),
        clientId=client_id,
        withdrawalStatus=resource_json.get('withdrawalStatus'),
        suspensionStatus=resource_json.get('suspensionStatus'))


def _get_primary_provider_link(participant):
  if participant.providerLink:
    provider_links = json.loads(participant.providerLink)
    if provider_links:
      for provider in provider_links:
        if provider.get('primary') == True:
          return provider
  return None


def _get_hpo_name_from_participant(participant):
  """Returns ExtractionResult with the string representing the HPO."""
  primary_provider_link = _get_primary_provider_link(participant)
  if primary_provider_link and primary_provider_link.get('organization'):
    reference = primary_provider_link.get('organization').get('reference')
    if reference and reference.lower().startswith('organization/'):
      return reference[13:]
  return None


def raise_if_withdrawn(obj):
  if obj.withdrawalStatus == WithdrawalStatus.NO_USE:
    raise Forbidden('Participant %d has withdrawn' % obj.participantId)
