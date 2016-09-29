'''The definition of the evaluation object and DB marshalling.
'''

import data_access_object
import participant

import fhirclient.models.bundle

from google.appengine.ext import ndb


class Evaluation(ndb.Model):
  """The evaluation resource definition"""
  resource = ndb.JsonProperty()

class EvaluationDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(EvaluationDAO, self).__init__(Evaluation, participant.Participant)

  def properties_from_json(self, dict_, ancestor_id, id_):
    model = fhirclient.models.bundle.Bundle(dict_)
    model.id = id_
    return {
      "resource": model.as_json()
    }

  def properties_to_json(self, dict_):
    return dict_['resource']

  def list(self, participant_id):
    p_key = ndb.Key(participant.Participant, participant_id)
    query = Evaluation.query(ancestor=p_key)
    return {"items": [self.to_json(p) for p in query.fetch()]}

DAO = EvaluationDAO()
