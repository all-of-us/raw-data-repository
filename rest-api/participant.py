'''The definition of the participant object and DB marshalling.
'''

import api_util
import copy

import data_access_object
import extraction
import identifier
import fhir_datatypes

from datetime import datetime
from dateutil.relativedelta import relativedelta
from protorpc import messages
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

# Valid values for the HPO, not currently enforced.
HPO_VALUES = (
    'pitt',        # Pitt/UPMC
    'columbia',    # Columbia University Medical Center
    'illinois',    # Illinois Precision Medicine Consortium
    'az_tucson',   # University of Arizona, Tucson
    'comm_health', # Community Health Center
    'san_ysidro',  # San Ysidro health Center, Inc.
    'cherokee',    # Cherokee Health Systems
    'eau_claire',  # Eau Claire Cooperative Health Centers, Inc
    'hrhcare',     # HRHCare (Hudson River Healthcare)
    'jackson',     # Jackson-Hinds Comprehensive Health Center
    'geisinger',   # Geisinger Health System
    'cal_pmc',     # California Precision Medicine Consortium
    'ne_pmc',      # New England Precision Medicine Consortium
    'trans_am',    # Trans-American Consortium for the Health Care Systems Research Network
    'va',          # Veterans Affairs
)

class ProviderLink(ndb.Model):
  """A link between a participant and an outside institution."""
  primary = ndb.BooleanProperty()
  organization = ndb.StructuredProperty(fhir_datatypes.FHIRReference, repeated=False)
  site = ndb.LocalStructuredProperty(fhir_datatypes.FHIRReference, repeated=True)
  identifier = ndb.LocalStructuredProperty(fhir_datatypes.FHIRIdentifier, repeated=True)

class Participant(ndb.Model):
  """The participant resource definition"""
  participantId = ndb.StringProperty()
  biobankId = ndb.StringProperty()
  # TODO: rename to lastModified (with data_access_object)
  last_modified = ndb.DateTimeProperty(auto_now=True)
  # Should this be indexed? If so, switch to StructuredProperty here and above
  # Should this be provider_link?
  providerLink = ndb.LocalStructuredProperty(ProviderLink, repeated=True)

  def get_primary_provider_link(self):
    if self.providerLink:
      for provider in self.providerLink:
        if provider.primary:
          return provider
    return None

class ParticipantDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParticipantDAO, self).__init__(Participant)

  def properties_from_json(self, dict_, ancestor_id, id_):
    if id_:
      dict_['participantId'] = id_
    return dict_

  def allocate_id(self):
    _id = identifier.get_id()
    return 'P{:d}'.format(_id).zfill(9)

  def insert(self, model, date=None, client_id=None):
    # Assign a new biobank ID when inserting a new participant
    model.biobankId = 'B{:d}'.format(identifier.get_id()).zfill(9)
    import participant_summary
    participant_key = ndb.Key(participant_summary.ParticipantSummary,
                          participant_summary.SINGLETON_SUMMARY_ID,
                          parent=model.key)
    summary = participant_summary.ParticipantSummary(key=participant_key,
                                                     participantId=model.participantId,
                                                     biobankId=model.biobankId)
    result = super(ParticipantDAO, self).insert(model, date, client_id)
    participant_summary.DAO.insert(summary, date, client_id)
    return result

  def find_participant_id_by_biobank_id(self, biobank_id):
    query = Participant.query(Participant.biobankId == biobank_id)
    results = query.fetch(options=ndb.QueryOptions(keys_only=True))
    if len(results) == 0:
      return None
    return results[0].id()

def extract_HPO_id(ph):
  """Returns ExtractionResult with the string representing the HPO."""
  primary_provider_link = ph.obj.get_primary_provider_link()
  if primary_provider_link and primary_provider_link.organization:
    return primary_provider_link.organization.value
  return 'UNSET'

def load_history_entities(participant_key, now):
  """Loads all related history entries.

  Details:
    - Loads all history objects for this participant.
    - Injects synthetic entries for when the participant's age changes.
    - Loads related QuestionnaireResponseHistory objects.
  """
  history = list(DAO.get_all_history(participant_key))
  modify_participant_history(history, participant_key, now)
  return history

def modify_participant_history(history, participant_key, now):
  """Modifies the participant history before summaries are created.
  This is used as part of the metrics pipeline to ensure that we capture when
  participant's age changes.
  """
  import questionnaire_response
  history.extend(questionnaire_response.DAO.get_all_history(participant_key))
  import evaluation
  history.extend(evaluation.DAO.get_all_history(participant_key))
  import biobank_order
  history.extend(biobank_order.DAO.get_all_history(participant_key))
  import biobank_sample
  samples = biobank_sample.DAO.load_if_present(biobank_sample.SINGLETON_SAMPLES_ID,
                                               participant_key.id())
  if samples:
    min_date = None
    for sample in samples.samples:
      if not min_date or min_date > sample.collectionDate:
        min_date = sample.collectionDate
    if min_date:
      samples.date = min_date
      history.append(samples)

DAO = ParticipantDAO()
