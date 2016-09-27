'''The definition of the evaluation object and DB marshalling.
'''

import copy

import api_util
import participant

from protorpc import message_types
from protorpc import messages
from google.appengine.ext import ndb


class Evaluation(ndb.Model):
  """The evaluation resource definition"""
  evaluation_id = ndb.StringProperty()
  participant_drc_id = ndb.StringProperty()
  completed = ndb.DateTimeProperty()
  evaluation_version = ndb.StringProperty()
  evaluation_data = ndb.StringProperty()

def from_json(json, participant_id, evaluation_id=None):
  json = copy.deepcopy(json)

  if evaluation_id:
    key = ndb.Key(participant.Participant, participant_id,
                  Evaluation, evaluation_id)
  else:
    key = ndb.Key(participant.Participant, participant_id)
  e = Evaluation(key=key)

  if 'completed' in json:
    json['completed'] = api_util.parse_date(json['completed'])

  e.populate(**json)
  return e

def to_json(e):
  dict = e.to_dict()
  dict = copy.deepcopy(dict)
  if dict['completed']:
    dict['completed'] = dict['completed'].isoformat()
  return dict

def list(participant_id):
  p_key = ndb.Key(participant.Participant, participant_id)
  query = Evaluation.query(ancestor=p_key)

  items = []
  for p in query.fetch():
    items.append(to_json(p))
  return {"items": items}
