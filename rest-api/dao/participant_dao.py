import clock
import extraction
import json
from dao.base_dao import BaseDao
from dao.hpo_dao import HPODao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant_summary import ParticipantSummary
from model.participant import Participant, ParticipantHistory
from participant_enums import UNSET_HPO_ID
from werkzeug.exceptions import BadRequest

class ParticipantHistoryDao(BaseDao):  
  def __init__(self):
    super(ParticipantHistoryDao, self).__init__(ParticipantHistory)

  def get_id(self, obj):
    return [obj.participantId, obj.version]

class ParticipantDao(BaseDao):
  
  def __init__(self):
    super(ParticipantDao, self).__init__(Participant)

  def get_id(self, obj):
    return obj.participantId
    
  def insert_with_session(self, session, obj):    
    obj.hpoId = self.get_hpo_id(session, obj)
    obj.signUpTime = clock.CLOCK.now()
    obj.lastModified = clock.CLOCK.now()
    super(ParticipantDao, self).insert_with_session(session, obj)
    obj.participantSummary = ParticipantSummary(participantId=obj.participantId, 
                                                biobankId=obj.biobankId,
                                                signUpTime=obj.signUpTime,
                                                hpoId=obj.hpoId)
    history = ParticipantHistory()
    history.fromdict(obj.asdict(), allow_pk=True)
    session.add(history)                                                

  def _update_history(self, session, obj):
    # Increment the version and add a new history entry.
    obj.version += 1
    history = ParticipantHistory()
    history.fromdict(obj.asdict(), allow_pk=True)
    session.add(history)

  def _do_update(self, session, obj, existing_obj):
    # If the provider link changes, update the HPO ID on the participant and its summary.
    obj.lastModified = clock.CLOCK.now()
    if obj.providerLink != existing_obj.providerLink:
      new_hpo_id = self.get_hpo_id(session, obj)
      if new_hpo_id != existing_obj.hpoId:
        obj.hpoId = new_hpo_id        
        self._update_history(session, obj)
        super(ParticipantDao, self)._do_update(session, obj, existing_obj)
        obj.participantSummary.hpoId = new_hpo_id
        return
    self._update_history(session, obj)
    super(ParticipantDao, self)._do_update(session, obj, existing_obj)

  def get_hpo_id(self, session, obj):
    hpo_name = get_HPO_name_from_participant(obj)
    if hpo_name:
      hpo = HPODao().get_by_name_with_session(session, hpo_name)
      if not hpo:
        raise BadRequest('No HPO found with name %s' % hpo_name)
      return hpo.hpoId if hpo else UNMAPPED_HPO_ID
    else:      
      return UNSET_HPO_ID
    
# TODO(danrodney): remove this logic from old participant code when done  
def get_primary_provider_link(participant):
  if participant.providerLink:
    provider_links = json.loads(participant.providerLink)    
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

    