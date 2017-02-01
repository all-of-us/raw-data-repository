'''The definition of the participant object and DB marshalling.
'''

import data_access_object
import extraction
import identifier
import fhir_datatypes

from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb
from offline.metrics_fields import run_extractors

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
    self.key = ndb.Key('AgeHistory', '')

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

  def make_participant_summary(self, model):
    import participant_summary
    participant_key = ndb.Key(participant_summary.ParticipantSummary,
                          participant_summary.SINGLETON_SUMMARY_ID,
                          parent=model.key)
    hpo_id_result = extract_HPO_id_from_participant(model)    
    return participant_summary.ParticipantSummary(key=participant_key,
                                                  participantId=model.key.id(),
                                                  biobankId=model.biobankId,
                                                  hpoId=hpo_id_result.value)

  @ndb.transactional
  def regenerate_summary(self, participant_key):
    participant = participant_key.get()
    if not participant:
      return None
    import participant_summary
    summary = self.make_participant_summary(participant)
    summary_json = participant_summary.DAO.to_json(summary)
    import questionnaire_response
    questionnaire_response_history = questionnaire_response.DAO.get_all_history(participant_key)
    questionnaire_response_history = sorted(questionnaire_response_history, key=lambda o: o.date)
    import field_config.participant_summary_config
    for qr_hist_obj in questionnaire_response_history:
      run_extractors(qr_hist_obj, field_config.participant_summary_config.CONFIG,
                     summary_json)
    import biobank_sample
    samples = biobank_sample.DAO.get_samples_for_participant(participant_key.id())
    if samples:
      run_extractors(samples, field_config.participant_summary_config.CONFIG, summary_json)
    
    existing_summary = participant_summary.DAO.get_summary_for_participant(participant_key.id())
    if existing_summary:
      # Transform the JSON to the model and back again, to make sure we get the same representation.
      adjusted_summary = participant_summary.DAO.from_json(summary_json, 
                                                           participant_summary.SINGLETON_SUMMARY_ID,
                                                           participant_key.id())
      adjusted_summary_json = participant_summary.DAO.to_json(adjusted_summary)

      existing_summary_json = participant_summary.DAO.to_json(existing_summary)
      # Clear out ageRange, since this doesn't get set until the summary is stored.
      existing_summary_json['ageRange'] = None
      if existing_summary_json == adjusted_summary_json:
        # If nothing has changed, bail out.
        return None
    updated_summary = participant_summary.DAO.from_json(summary_json,
                                                        participant_key.id(),
                                                        participant_summary.SINGLETON_SUMMARY_ID)
    participant_summary.DAO.store(updated_summary)
    return updated_summary

  def insert(self, model, date=None, client_id=None):
    # Assign a new biobank ID when inserting a new participant
    model.biobankId = 'B{:d}'.format(identifier.get_id()).zfill(9)
    summary = self.make_participant_summary(model)
    result = super(ParticipantDAO, self).insert(model, date, client_id)
    import participant_summary
    participant_summary.DAO.insert(summary, date, client_id)
    return result
    
  def update(self, model, expected_version_id, date=None, client_id=None):    
    result = super(ParticipantDAO, self).update(model, expected_version_id, date, client_id)
    import participant_summary
    existing_summary = participant_summary.DAO.get_summary_for_participant(model.key.id())    
    new_hpo_id = extract_HPO_id_from_participant(model)
    if new_hpo_id.value != existing_summary.hpoId:
      participant_summary.DAO.update_hpo_id(model.key.id(), new_hpo_id.value)
    return result

  def find_participant_id_by_biobank_id(self, biobank_id):
    query = Participant.query(Participant.biobankId == biobank_id)
    results = query.fetch(options=ndb.QueryOptions(keys_only=True))
    if len(results) == 0:
      return None
    return results[0].id()

def extract_HPO_id(ph):
  return extract_HPO_id_from_participant(ph.obj)
  
def extract_HPO_id_from_participant(participant):
  """Returns ExtractionResult with the string representing the HPO."""
  primary_provider_link = participant.get_primary_provider_link()
  import participant_summary
  if (primary_provider_link and primary_provider_link.organization and 
      primary_provider_link.organization.reference and
      primary_provider_link.organization.reference.lower().startswith('organization/')):
    hpo_id_string = primary_provider_link.organization.reference[13:]
    if participant_summary.HPOId.to_dict().get(hpo_id_string):
      return extraction.ExtractionResult(participant_summary.HPOId(hpo_id_string), True)
    else:
      return extraction.ExtractionResult(participant_summary.HPOId.UNMAPPED, True)
  return extraction.ExtractionResult(participant_summary.HPOId.UNSET, True)

def load_history_entities(participant_key, now):
  """Loads all related history entries.

  Details:
    - Loads all history objects for this participant.
    - Injects synthetic entries for when the participant's age changes.
    - Loads related QuestionnaireResponseHistory objects.
  """
  history = list(DAO.get_all_history(participant_key, now))
  if not history:
    return history
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
    history.append(BirthdayEvent(summary.dateOfBirth, history[0].date))
    difference_in_years = relativedelta(history[0].date, summary.dateOfBirth).years

    year = relativedelta(years=1)
    date = summary.dateOfBirth + relativedelta(years=difference_in_years + 1)
    while date and date < now.date():
      age_history_obj = BirthdayEvent(
          summary.dateOfBirth, datetime.combine(date, datetime.min.time()))
      history.append(age_history_obj)
      date = date + year

  import questionnaire_response
  history.extend(questionnaire_response.DAO.get_all_history(participant_key, now))
  import measurements
  history.extend(measurements.DAO.get_all_history(participant_key, now))
  import biobank_order
  history.extend(biobank_order.DAO.get_all_history(participant_key, now))
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
