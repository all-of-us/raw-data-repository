import logging

import data_access_object

import extraction

from questionnaire import DAO as QuestionnaireDAO

from extraction import QuestionnaireExtractor, QuestionnaireResponseExtractor

import fhirclient.models.questionnaireresponse
from google.appengine.ext import ndb
from participant import Participant

class QuestionnaireResponse(ndb.Model):
  """The questionnaire response."""
  resource = ndb.JsonProperty()

class QuestionnaireResponseDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(QuestionnaireResponseDAO, self).__init__(QuestionnaireResponse, Participant)

  def properties_to_json(self, m):
    return m['resource']

  def properties_from_json(self, dict_, ancestor_id, id_):
    model = fhirclient.models.questionnaireresponse.QuestionnaireResponse(dict_)
    model.id = id_
    return {
        "resource": model.as_json()
    }


def extract_race(history_object):
  return extract_field(history_object, extraction.RACE_CONCEPT)

def extract_ethnicity(history_object):
  return extract_field(history_object, extraction.ETHNICITY_CONCEPT)

def extract_field(history_object, concept):
  resource = history_object.obj.resource
  response_extractor = QuestionnaireResponseExtractor(resource)
  questionnaire_id = response_extractor.extract_questionnaire_id()
  logging.info('Looking up questionnaire id %s' % questionnaire_id)
  questionnaire = QuestionnaireDAO.load_if_present(questionnaire_id)
  if not questionnaire:
    raise ValueError(
        'Invalid Questionnaire id {0} in Response {1}'.format(
            questionnaire_id, response_extractor.extract_id()))
  questionnaire_extractor = QuestionnaireExtractor(questionnaire.resource)
  link_id = questionnaire_extractor.extract_link_id_for_concept(concept)
  return response_extractor.extract_answer(link_id, concept)


DAO = QuestionnaireResponseDAO()
