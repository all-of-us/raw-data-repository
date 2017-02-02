'''The definition of the participant summary object and DB marshalling.
'''

import api_util
import clock
import copy
import data_access_object
import singletons

from offline.metrics_fields import run_extractors
from participant_enums import HPOId, PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import MembershipTier, GenderIdentity, Ethnicity, Race, get_bucketed_age
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from werkzeug.exceptions import BadRequest

DATE_OF_BIRTH_FORMAT = '%Y-%m-%d'
SINGLETON_SUMMARY_ID = '1'

class ParticipantSummary(ndb.Model):
  """The participant summary resource definition.
  
  Participant summaries are a denormalized view of Participants and a number of related entities,
  including QuestionnaireResponses and BioBank orders. Used to sort/filter participants for 
  HealthPro work queues. This is effectively the output of a complex join.
  """
  participantId = ndb.StringProperty()
  biobankId = ndb.StringProperty()
  firstName = ndb.StringProperty(indexed=False)
  firstNameSearch = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.firstName))
  middleName = ndb.StringProperty(indexed=False)
  middleNameSearch = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.middleName))
  lastName = ndb.StringProperty(indexed=False)
  lastNameSearch = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.lastName))
  zipCode = ndb.StringProperty()
  dateOfBirth = ndb.DateProperty()
  sortKey = ndb.ComputedProperty(
      lambda self: "|".join([
          self.lastNameSearch or '',
          self.firstNameSearch or '',
          self.middleNameSearch or '',
          self.dateOfBirth.isoformat() if self.dateOfBirth else '']))
  # Don't use a ComputedProperty here, as a dynamic expression in ComputedProperty is evaluated
  # at both write and read time, and can thus result in queries for one value returning
  # an entity with another value
  ageRange = ndb.StringProperty()
  genderIdentity = msgprop.EnumProperty(GenderIdentity, default=GenderIdentity.UNSET)
  membershipTier = msgprop.EnumProperty(MembershipTier, default=MembershipTier.UNSET)
  race = msgprop.EnumProperty(Race, default=Race.UNSET)
  ethnicity = msgprop.EnumProperty(Ethnicity, default=Ethnicity.UNSET)
  physicalMeasurementsStatus = msgprop.EnumProperty(
      PhysicalMeasurementsStatus, default=PhysicalMeasurementsStatus.UNSET, indexed=False)
  signUpTime = ndb.DateTimeProperty(indexed=False)
  hpoId = msgprop.EnumProperty(HPOId, default=HPOId.UNSET)
  consentForStudyEnrollment = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  consentForStudyEnrollmentTime = ndb.DateTimeProperty(indexed=False)
  consentForElectronicHealthRecords = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  consentForElectronicHealthRecordsTime = ndb.DateTimeProperty(indexed=False)
  questionnaireOnOverallHealth = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnOverallHealthTime = ndb.DateTimeProperty(indexed=False)
  questionnaireOnPersonalHabits = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnPersonalHabitsTime = ndb.DateTimeProperty(indexed=False)      
  questionnaireOnSociodemographics = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnSociodemographicsTime = ndb.DateTimeProperty(indexed=False)
  questionnaireOnHealthcareAccess = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnHealthcareAccessTime = ndb.DateTimeProperty(indexed=False)
  questionnaireOnMedicalHistory = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnMedicalHistoryTime = ndb.DateTimeProperty(indexed=False)
  questionnaireOnMedications = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnMedicationsTime = ndb.DateTimeProperty(indexed=False)
  questionnaireOnFamilyHealth = msgprop.EnumProperty(
      QuestionnaireStatus, default=QuestionnaireStatus.UNSET, indexed=False)
  questionnaireOnFamilyHealthTime = ndb.DateTimeProperty(indexed=False)
  numCompletedBaselinePPIModules = ndb.IntegerProperty(default=0)
  numBaselineSamplesArrived = ndb.IntegerProperty(default=0)

_HPO_FILTER_FIELDS = [
    "hpoId", "firstName", "middleName", "lastName", "dateOfBirth", "ageRange", "genderIdentity",
    "ethnicity", "zipCode", "membershipTier", "consentForStudyEnrollment",
    "numCompletedBaselinePPIModules", "numBaselineSamplesArrived"]
_NON_HPO_FILTER_FIELDS = [
    "firstName", "lastName", "dateOfBirth", "genderIdentity", "zipCode"]

class ParticipantSummaryDAO(data_access_object.DataAccessObject):

  def __init__(self):
    import participant
    super(ParticipantSummaryDAO, self).__init__(ParticipantSummary, participant.Participant,
                                                keep_history=False)

  def properties_from_json(self, dict_, ancestor_id, id_):
    dict_['participantId'] = ancestor_id
    api_util.parse_json_date(dict_, 'dateOfBirth', DATE_OF_BIRTH_FORMAT)
    api_util.parse_json_date(dict_, 'signUpTime')
    api_util.parse_json_date(dict_, 'consentForStudyEnrollmentTime')
    api_util.parse_json_date(dict_, 'consentForElectronicHealthRecordsTime')
    api_util.parse_json_date(dict_, 'questionnaireOnOverallHealthTime')
    api_util.parse_json_date(dict_, 'questionnaireOnPersonalHabitsTime')
    api_util.parse_json_date(dict_, 'questionnaireOnSociodemographicsTime')
    api_util.parse_json_date(dict_, 'questionnaireOnHealthcareAccessTime')
    api_util.parse_json_date(dict_, 'questionnaireOnMedicalHistoryTime')
    api_util.parse_json_date(dict_, 'questionnaireOnMedicationsTime')
    api_util.parse_json_date(dict_, 'questionnaireOnFamilyHealthTime')
    api_util.parse_json_enum(dict_, 'hpoId', HPOId)
    api_util.parse_json_enum(dict_, 'genderIdentity', GenderIdentity)
    api_util.parse_json_enum(dict_, 'race', Race)
    api_util.parse_json_enum(dict_, 'ethnicity', Ethnicity)
    api_util.parse_json_enum(dict_, 'membershipTier', MembershipTier)
    api_util.parse_json_enum(dict_, 'physicalMeasurementsStatus', PhysicalMeasurementsStatus)
    api_util.parse_json_enum(dict_, 'consentForStudyEnrollment', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'consentForElectronicHealthRecords', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnOverallHealth', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnPersonalHabits', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnSociodemographics', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnHealthcareAccess', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnMedicalHistory', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnMedications', QuestionnaireStatus)
    api_util.parse_json_enum(dict_, 'questionnaireOnFamilyHealth', QuestionnaireStatus)
    api_util.remove_field(dict_, 'ageRange')
    api_util.remove_field(dict_, 'samplesArrived')
    return dict_

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'dateOfBirth', DATE_OF_BIRTH_FORMAT)
    api_util.format_json_date(dict_, 'signUpTime')
    api_util.format_json_date(dict_, 'consentForStudyEnrollmentTime')
    api_util.format_json_date(dict_, 'consentForElectronicHealthRecordsTime')
    api_util.format_json_date(dict_, 'questionnaireOnOverallHealthTime')
    api_util.format_json_date(dict_, 'questionnaireOnPersonalHabitsTime')
    api_util.format_json_date(dict_, 'questionnaireOnSociodemographicsTime')
    api_util.format_json_date(dict_, 'questionnaireOnHealthcareAccessTime')
    api_util.format_json_date(dict_, 'questionnaireOnMedicalHistoryTime')
    api_util.format_json_date(dict_, 'questionnaireOnMedicationsTime')
    api_util.format_json_date(dict_, 'questionnaireOnFamilyHealthTime')
    api_util.format_json_enum(dict_, 'hpoId')
    api_util.format_json_enum(dict_, 'genderIdentity')
    api_util.format_json_enum(dict_, 'race')
    api_util.format_json_enum(dict_, 'ethnicity')
    api_util.format_json_enum(dict_, 'membershipTier')
    api_util.format_json_enum(dict_, 'physicalMeasurementsStatus')
    api_util.format_json_enum(dict_, 'consentForStudyEnrollment')
    api_util.format_json_enum(dict_, 'consentForElectronicHealthRecords')
    api_util.format_json_enum(dict_, 'questionnaireOnOverallHealth')
    api_util.format_json_enum(dict_, 'questionnaireOnPersonalHabits')
    api_util.format_json_enum(dict_, 'questionnaireOnSociodemographics')
    api_util.format_json_enum(dict_, 'questionnaireOnHealthcareAccess')
    api_util.format_json_enum(dict_, 'questionnaireOnMedicalHistory')
    api_util.format_json_enum(dict_, 'questionnaireOnMedications')
    api_util.format_json_enum(dict_, 'questionnaireOnFamilyHealth')
    api_util.remove_field(dict_, 'firstNameSearch')
    api_util.remove_field(dict_, 'middleNameSearch')
    api_util.remove_field(dict_, 'lastNameSearch')
    api_util.remove_field(dict_, 'sortKey')
    return dict_

  def validate_query(self, query_definition):
    field_names = set(filter.field_name for filter in query_definition.field_filters)
    if 'hpoId' in field_names:
      for field_filter in query_definition.field_filters:
        if not field_filter.field_name in _HPO_FILTER_FIELDS:
          raise BadRequest("Invalid filter on field %s" % field_filter.field_name)
    else:
      if 'lastName' in field_names and 'dateOfBirth' in field_names:
        for field_filter in query_definition.field_filters:
          if not field_filter.field_name in _NON_HPO_FILTER_FIELDS:
            raise BadRequest("Invalid filter on field %s without HPO ID" % field_filter.field_name)
      else:
        raise BadRequest("Participant summary queries must specify hpoId"
                         " or both lastName and dateOfBirth")

  def get_summary_for_participant(self, participant_id):
    return self.load_if_present(SINGLETON_SUMMARY_ID, participant_id)

  @ndb.transactional
  def store(self, model, date=None, client_id=None):
    # Set the age range based on the current date when the summary is stored.
    model.ageRange = get_bucketed_age(model.dateOfBirth, clock.CLOCK.now())
    super(ParticipantSummaryDAO, self).store(model, date, client_id)

  @ndb.transactional
  def update_hpo_id(self, participant_id, hpo_id):
    summary = self.get_summary_for_participant(participant_id)
    summary.hpoId = hpo_id
    self.store(summary)
    
  @ndb.transactional
  def update_with_incoming_data(self, participant_id, incoming_history_obj, config):
    old_summary = self.get_summary_for_participant(participant_id)
    old_summary_json = self.to_json(old_summary)
    new_summary = copy.deepcopy(old_summary_json)
    run_extractors(incoming_history_obj, config, new_summary)
    
    # If the extracted fields don't match, update them
    changed = False
    for field_name, value in new_summary.iteritems():
      old_value = old_summary_json.get(field_name)

      if value != old_value:
        old_summary_json[field_name] = value
        changed = True
    if changed:
      updated_summary = self.from_json(old_summary_json,
                                       old_summary.key.parent().id(),
                                       SINGLETON_SUMMARY_ID)
      self.store(updated_summary)

def DAO():
  return singletons.get(ParticipantSummaryDAO)
