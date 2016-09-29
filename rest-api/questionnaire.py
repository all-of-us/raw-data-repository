'''The definition of the questionnaire object and DB marshalling.
'''

import fhirclient.models.questionnaire

import data_access_object

from google.appengine.ext import ndb


class Questionnaire(ndb.Model):
  """The questionnaire."""
  resource = ndb.JsonProperty()

class QuestionnaireDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(QuestionnaireDAO, self).__init__(Questionnaire)

  def fields_to_json(self, m):
    return m['resource']

  def fields_from_json(self, dict, ancestor_id=None, id=None):
    model = fhirclient.models.questionnaire.Questionnaire(dict)
    if id:
      model.id = id
    return {
        "resource": model.as_json()
    }

DAO = QuestionnaireDAO()
