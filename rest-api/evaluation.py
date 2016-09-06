'''The definition of the evaluation object and DB marshalling.
'''
import db
import data_access_object

from protorpc import message_types
from protorpc import messages


KEY_COLUMNS = [
    'evaluation_id',
    'participant_id',
]

# For now, the evaluation fields map directly to the db columns, so do a simple
# mapping.
COLUMNS = KEY_COLUMNS + [
    'completed',
    'evaluation_version',
    'evaluation_data',
]


class EvaluationResource(messages.Message):
  """The evaluation resource definition"""
  evaluation_id = messages.StringField(1)
  participant_id = messages.StringField(2)
  completed = message_types.DateTimeField(3)
  evaluation_version = messages.StringField(4)
  evaluation_data = messages.StringField(5)


class EvaluationCollection(messages.Message):
  """Collection of Evaluations."""
  items = messages.MessageField(EvaluationResource, 1, repeated=True)


class Evaluation(data_access_object.DataAccessObject):
  def __init__(self):
    super(Evaluation, self).__init__(resource=EvaluationResource,
                                     collection=EvaluationCollection,
                                     table='evaluation',
                                     columns=COLUMNS,
                                     key_columns=KEY_COLUMNS)
