'''The definition of the questionnaire object and DB marshalling.
'''
from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages



class Coding(messages.Message):
  system = messages.StringField(1)
  version = messages.StringField(2)
  code = messages.StringField(3)
  display = messages.StringField(4)
  userSelected = messages.StringField(5)


class Reference(messages.Message):
  reference = messages.StringField(1)
  display = messages.StringField(2)


QUESTION_KEY_COLUMNS = ('question_id', 'questionnaire_id')

QUESTION_COLUMNS = QUESTION_KEY_COLUMNS + (
    'linkId',
    'concept',
    'text',
    'type',
    'required',
    'repeats',
    'options',
    'option_col',
)

class QuestionResource(messages.Message):
  question_id = messages.StringField(1)
  questionnaire_id = messages.StringField(2)
  linkId = messages.StringField(3)
  concept = messages.MessageField(Coding, 4, repeated=True)
  text = messages.StringField(5)
  type = messages.StringField(6)
  required = messages.BooleanField(7)
  repeats = messages.BooleanField(8)
  options = messages.MessageField(Reference, 9, repeated=False)
  option = messages.StringField(10)
  group = messages.MessageField('QuestionResource', 11, repeated=True)

class Question(DataAccessObject):
  def __init__(self):
    # Option is a keyword in MySQL, we have to map it to option_col.
    super(Question, self).__init__(resource=QuestionResource,
                                   table='question',
                                   columns=QUESTION_COLUMNS,
                                   key_columns=QUESTION_KEY_COLUMNS,
                                   column_map={'option_col': 'option'})



QUESTIONNAIRE_GROUP_KEY_COLUMNS = ('questionnaire_group_id', 'questionnaire_id')
QUESTIONNAIRE_GROUP_COLUMNS = QUESTION_KEY_COLUMNS + (
    'linkId',
    'concept',
    'text',
    'type',
    'required',
    'repeats',
    'group',
)

class QuestionnaireGroupResource(messages.Message):
  """A group of questions in a questionnaire."""
  linkId = messages.StringField(1)
  concept = messages.MessageField(Coding, 2, repeated=True)
  text = messages.StringField(3)
  type = messages.StringField(4)
  required = messages.BooleanField(5)
  repeats = messages.BooleanField(6)
  group = messages.MessageField('QuestionnaireGroupResource', 7, repeated=True)
  question = messages.MessageField(QuestionResource, 8, repeated=True)

class QuestionnaireGroup(DataAccessObject):
  def __init__(self):
    super(Question, self).__init__(resource=QuestionResource,
                                   table='question',
                                   columns=QUESTIONNAIRE_GROUP_COLUMNS,
                                   key_columns=QUESTIONNAIRE_GROUP_KEY_COLUMNS)




QUESTIONNAIRE_KEY_COLUMNS = ('id',)
QUESTIONNAIRE_COLUMNS = QUESTIONNAIRE_KEY_COLUMNS + (
    'identifier',
    'version',
    'status',
    'date',
    'publisher',
    'telecom',
)


class QuestionnaireResource(messages.Message):
  """The questionnaire resource definition"""
  id = messages.StringField(1)
  identifier = messages.StringField(2)
  version = messages.StringField(3)
  status = messages.StringField(4)
  date = messages.StringField(5)
  publisher = messages.StringField(6)
  telecom = messages.StringField(7)
  subjectType = messages.StringField(8)
  group = messages.MessageField(QuestionnaireGroupResource, 9, repeated=False)


class QuestionnaireCollection(messages.Message):
  """Collection of Questionnaires."""
  items = messages.MessageField(QuestionnaireResource, 1, repeated=True)


class Questionnaire(DataAccessObject):
  def __init__(self):
    super(Questionnaire, self).__init__(resource=QuestionnaireResource,
                                        table='questionnaire',
                                        columns=QUESTIONNAIRE_COLUMNS,
                                        key_columns=QUESTIONNAIRE_KEY_COLUMNS)
