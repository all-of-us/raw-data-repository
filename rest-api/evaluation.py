'''The definition of the evaluation object and DB marshalling.
'''

import api_util
import data_access_object
import participant

from google.appengine.ext import ndb


class Evaluation(ndb.Model):
  """The evaluation resource definition"""
  evaluation_id = ndb.StringProperty()
  participant_drc_id = ndb.StringProperty()
  completed = ndb.DateTimeProperty()
  evaluation_version = ndb.StringProperty()
  evaluation_data = ndb.StringProperty()

class EvaluationDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(EvaluationDAO, self).__init__(Evaluation, participant.Participant)

  def fields_from_json(self, dict, ancestor_id=None, id=None):
    if id:
      dict['evaluation_id'] = id

    if 'completed' in dict:
      dict['completed'] = api_util.parse_date(dict['completed'])
    return dict

  def fields_to_json(self, dict):
    if dict['completed']:
      dict['completed'] = dict['completed'].isoformat()
    return dict

  def list(self, participant_id):
    p_key = ndb.Key(participant.Participant, participant_id)
    query = Evaluation.query(ancestor=p_key)

    items = []
    for p in query.fetch():
      items.append(self.to_json(p))
    return {"items": items}

DAO = EvaluationDAO()
