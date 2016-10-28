'''The definition of the participant object and DB marshalling.
'''

import api_util
import copy

import data_access_object
import extraction
import identifier

from datetime import datetime
from dateutil.relativedelta import relativedelta
from protorpc import messages
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

DATE_OF_BIRTH_FORMAT = '%Y-%m-%d'

class PhysicalEvaluationStatus(messages.Enum):
  """The state of the participant's physical evaluation"""
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3


class MembershipTier(messages.Enum):
  """The state of the participant"""
  REGISTERED = 1
  VOLUNTEER = 2
  FULL_PARTICIPANT = 3
  ENROLLEE = 4
  # Note that these are out of order; ENROLEE was added after FULL_PARTICIPANT.

class GenderIdentity(messages.Enum):
  """The gender identity of the participant."""
  FEMALE = 1
  MALE = 2
  FEMALE_TO_MALE_TRANSGENDER = 3
  MALE_TO_FEMALE_TRANSGENDER = 4
  INTERSEX = 5
  OTHER = 6
  PREFER_NOT_TO_SAY = 7

class RecruitmentSource(messages.Enum):
  HPO = 1
  DIRECT_VOLUNTEER = 2


class Participant(ndb.Model):
  """The participant resource definition"""
  participant_id = ndb.StringProperty()
  biobank_id = ndb.StringProperty()
  first_name = ndb.StringProperty()
  first_name_search = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.first_name))
  middle_name = ndb.StringProperty()
  last_name = ndb.StringProperty()
  last_name_search = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.last_name))
  zip_code = ndb.StringProperty()
  date_of_birth = ndb.DateProperty()
  gender_identity = msgprop.EnumProperty(GenderIdentity)
  membership_tier = msgprop.EnumProperty(MembershipTier)
  physical_evaluation_status = msgprop.EnumProperty(PhysicalEvaluationStatus)
  sign_up_time = ndb.DateTimeProperty()
  consent_time = ndb.DateTimeProperty()
  hpo_id = ndb.StringProperty()
  recruitment_source = msgprop.EnumProperty(RecruitmentSource)


class ParticipantDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParticipantDAO, self).__init__(Participant)

  def properties_from_json(self, dict_, ancestor_id, id_):
    if id_:
      dict_['participant_id'] = id_
    api_util.parse_json_date(dict_, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
    api_util.parse_json_date(dict_, 'sign_up_time')
    api_util.parse_json_date(dict_, 'consent_time')
    api_util.parse_json_enum(dict_, 'gender_identity', GenderIdentity)
    api_util.parse_json_enum(dict_, 'membership_tier', MembershipTier)
    api_util.parse_json_enum(dict_, 'physical_evaluation_status', PhysicalEvaluationStatus)
    api_util.parse_json_enum(dict_, 'recruitment_source', RecruitmentSource)
    return dict_

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
    api_util.format_json_date(dict_, 'sign_up_time')
    api_util.format_json_date(dict_, 'consent_time')
    api_util.format_json_enum(dict_, 'gender_identity')
    api_util.format_json_enum(dict_, 'membership_tier')
    api_util.format_json_enum(dict_, 'physical_evaluation_status')
    api_util.format_json_enum(dict_, 'recruitment_source')
    api_util.remove_field(dict_, 'first_name_search')
    api_util.remove_field(dict_, 'last_name_search')
    return dict_


  def list(self, first_name, last_name, dob_string, zip_code):
    date_of_birth = api_util.parse_date(dob_string, DATE_OF_BIRTH_FORMAT)
    query = Participant.query(
        Participant.last_name_search == api_util.searchable_representation(last_name),
        Participant.date_of_birth == date_of_birth)
    if first_name:
      query = query.filter(
          Participant.first_name_search == api_util.searchable_representation(first_name))

    if zip_code:
      query = query.filter(Participant.zip_code == zip_code)

    items = []
    for p in query.fetch():
      items.append(self.to_json(p))
    return {"items": items}

  def allocate_id(self):
    _id = identifier.get_id()
    return 'P{:d}'.format(_id).zfill(9)
    
  def insert(self, model, date=None, client_id=None):
    # Assign a new biobank ID when inserting a new participant 
    model.biobank_id = 'B{:d}'.format(identifier.get_id()).zfill(9)
    return super(ParticipantDAO, self).insert(model, date, client_id)      

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
  inject_age_change_records(history, now)
  insert_questionnaire_responses(participant_key, history)

def insert_questionnaire_responses(participant_key, history):
  """Joins in the questionnaire response history for this participant."""
  import questionnaire_response
  history.extend(questionnaire_response.DAO.get_all_history(participant_key))

def inject_age_change_records(history, now):
  """Inject history records when a participant's age changes.

 Args:
  history: The list of history objects for this participant.  This function
      assumes that this list is sorted chronologically.
  now: The datetime to use for "now".

  """
  # In the case of database upgrade, we may not get history entries.
  if not history:
    return

  # Assume that the birthdate on the newest history record is the correct one.
  date = history[-1].obj.date_of_birth

  # Don't inject any history records before the first existing one.
  start_date = history[0].date
  year = relativedelta(years=1)
  dates_to_inject = []
  while date and date < now.date():
    if date > start_date.date():
      dates_to_inject.append(date)
    date = date + year

  new_objs = []
  for hist_obj in reversed(history):
    while dates_to_inject and dates_to_inject[-1] > hist_obj.date.date():
      new_obj = copy.deepcopy(hist_obj)
      new_obj.date = datetime.combine(dates_to_inject[-1], datetime.min.time())
      new_objs.append(new_obj)
      del dates_to_inject[-1]

  history.extend(new_objs)

def extract_age(participant_hist_obj, age_func):
  """Returns ExtractionResult with the bucketed participant age on that date."""
  today = participant_hist_obj.date
  participant = participant_hist_obj.obj
  if not participant.date_of_birth:
    return extraction.ExtractionResult(None)  # DOB was not provided: set None
  return extraction.ExtractionResult(age_func(participant.date_of_birth, today))

def extract_HPO_id(ph):
  """Returns ExtractionResult with the string representing the HPO."""
  return extraction.ExtractionResult(
      ((ph.obj.recruitment_source and (str(ph.obj.recruitment_source) + ':')
        or '')
       + str(ph.obj.hpo_id)))


DAO = ParticipantDAO()
