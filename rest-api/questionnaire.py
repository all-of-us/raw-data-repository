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

  def properties_to_json(self, m):
    return m['resource']

  def properties_from_json(self, dict_, ancestor_id, id_):
    model = fhirclient.models.questionnaire.Questionnaire(dict_)
    model.id = id_
    return {
        "resource": model.as_json()
    }

DAO = QuestionnaireDAO()
