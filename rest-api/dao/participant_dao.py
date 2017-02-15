import extraction
import json
from dao.base_dao import BaseDao
from dao.hpo_dao import HPODao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant_summary import ParticipantSummary
from model.participant import Participant
from participant_enums import UNSET, UNSET_HPO_ID, UNMAPPED_HPO_ID

class ParticipantDao(BaseDao):
  
  def __init__(self):
    super(ParticipantDao, self).__init__(Participant)

  def get_id(self, obj):
    return obj.participantId
    
  def insert_with_session(self, session, obj):    
    super(ParticipantDao, self).insert_with_session(session, obj)
    obj.hpoId = self.get_hpo_id(session, obj)
    obj.participantSummary = ParticipantSummary(participantId=obj.participantId, 
                                                biobankId=obj.biobankId,
                                                signUpTime=obj.signUpTime,
                                                hpoId=obj.hpoId)                                                

  def do_update(self, session, obj, existing_obj):
    # If the provider link changes, update the HPO ID on the participant and its summary.
    if obj.providerLink != existing_obj.providerLink:
      new_hpo_id = self.get_hpo_id(session, obj)
      if new_hpo_id != existing_obj.hpoId:
        obj.hpoId = new_hpo_id
        obj.participantSummary.hpoId = new_hpo_id      
    super(ParticipantDao, self).do_update(session, obj, existing_obj)

  def get_hpo_id(self, session, obj):
    hpo_name = get_HPO_name_from_participant(obj)
    if hpo_name:
      hpo = HPODao().get_by_name_with_session(session, hpo_name)
      # Return 1 for UNMAPPED if the name doesn't resolve to anything.
      return hpo.hpoId if hpo else UNMAPPED_HPO_ID
    else:      
      return UNSET_HPO_ID
    
# TODO(danrodney): remove this logic from old participant code when done  
def get_primary_provider_link(participant):
  if participant.providerLink:
    for provider in json.loads(participant.providerLink):
      if provider.primary:
        return provider
  return None
  
def get_HPO_name_from_participant(participant):
  """Returns ExtractionResult with the string representing the HPO."""
  primary_provider_link = get_primary_provider_link(participant)  
  if (primary_provider_link and primary_provider_link.organization and 
      primary_provider_link.organization.reference and
      primary_provider_link.organization.reference.lower().startswith('organization/')):
    hpo_id_string = primary_provider_link.organization.reference[13:]
    return hpo_id_string    
  return None

    