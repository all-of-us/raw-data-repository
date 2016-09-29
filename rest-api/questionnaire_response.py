
import data_access_object

import fhirclient.models.questionnaireresponse
from google.appengine.ext import ndb


class QuestionnaireResponse(ndb.Model):
  """The questionnaire response."""
  resource = ndb.JsonProperty()

class QuestionnaireResponseDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(QuestionnaireResponseDAO, self).__init__(QuestionnaireResponse)

  def fields_to_json(self, m):
    return m['resource']

  def fields_from_json(self, dict, ancestor_id=None, id=None):
    model = fhirclient.models.questionnaireresponse.QuestionnaireResponse(dict)
    if id:
      model.id = id
    return {
        "resource": model.as_json()
    }

DAO = QuestionnaireResponseDAO()
