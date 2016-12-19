'''The definition of the participant object and DB marshalling.
'''

import data_access_object
import extraction
import identifier
import fhir_datatypes

from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb

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

# Fake history entry created to represent the age of the participant
class BirthdayEvent(object):
  def __init__(self, date_of_birth, date):
    self.date_of_birth = date_of_birth
    self.date = date
    self.obj = Participant()
    self.key = ndb.Key('ParticipantHistory', '')

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
    return extraction.ExtractionResult(primary_provider_link.organization.reference, True)
  return extraction.ExtractionResult(None, False)

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
  
  # Set initial date of birth, and insert BirthdayEvent entries for each birthday after
  # the participant's creation until today.
  import participant_summary
  summary = participant_summary.DAO.get_summary_for_participant(participant_key.id())
  if summary and summary.dateOfBirth:
    history[0].date_of_birth = summary.dateOfBirth
    difference_in_years = relativedelta(history[0].date, summary.dateOfBirth).years
        
    year = relativedelta(years=1)
    date = summary.dateOfBirth + relativedelta(years=difference_in_years + 1)
    while date and date < now.date():
      age_history_obj = BirthdayEvent(summary.dateOfBirth, datetime.combine(date, datetime.min.time()))
      history.append(age_history_obj)    
      date = date + year
    
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
