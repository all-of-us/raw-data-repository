import collections
import uuid

import fhir_resources
import questionnaire

from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages

ANSWER_KEY_COLUMNS = ('answer_id', 'questionnaire_response_id')

ANSWER_COLUMNS = ANSWER_KEY_COLUMNS + ('parent_id',
                                       'ordinal',
                                       'valueBoolean',
                                       'valueDecimal',
                                       'valueInteger',
                                       'valueDate',
                                       'valueDateTime',
                                       'valueInstant',
                                       'valueTime',
                                       'valueString',
                                       'valueUri',
                                       'valueAttachment',
                                       'valueCoding',
                                       'valueQuantity',
                                       'valueReference')


class AnswerResource(messages.Message):
  answer_id = messages.StringField(1)
  parent_id = messages.StringField(2)
  questionnaire_response_id = messages.StringField(3)
  ordinal = messages.IntegerField(4)
  valueBoolean = messages.BooleanField(5)
  valueDecimal = messages.FloatField(6)
  valueInteger = messages.IntegerField(7)
  valueDate = message_types.DateTimeField(8)
  valueDateTime = message_types.DateTimeField(9)
  valueInstant = message_types.DateTimeField(10)
  valueTime = message_types.DateTimeField(11)
  valueString = messages.StringField(12)
  valueUri = messages.StringField(13)
  valueAttachment = messages.MessageField(fhir_resources.AttachmentResource, 14,
                                          repeated=False)
  valueCoding = messages.MessageField(fhir_resources.CodingResource, 15,
                                      repeated=False)
  valueQuantity = messages.MessageField(fhir_resources.QuantityResource, 16,
                                        repeated=False)
  valueReference = messages.MessageField(fhir_resources.ReferenceResource, 17,
                                         repeated=False)
  group = messages.MessageField('QuestionnaireResponseGroupResource', 18,
                               repeated=True)

class Answer(DataAccessObject):
  def __init__(self):
    super(Answer, self).__init__(resource=AnswerResource,
                                   table='answer',
                                   columns=ANSWER_COLUMNS,
                                   key_columns=ANSWER_KEY_COLUMNS)
    self.set_synthetic_fields(ANSWER_KEY_COLUMNS + ('parent_id', 'ordinal',))

  def link(self, obj, parent, ordinal):
    obj.parent_id = parent.question_response_id
    obj.questionnaire_response_id = parent.questionnaire_response_id
    obj.ordinal = ordinal
    if not obj.answer_id:
      obj.answer_id = str(uuid.uuid4())


QUESTION_RESPONSE_KEY_COLUMNS = ('question_response_id',
                                 'questionnaire_response_id')
QUESTION_RESPONSE_COLUMNS = QUESTION_RESPONSE_KEY_COLUMNS + (
    'ordinal', 'parent_id', 'linkId', 'text')
class QuestionResponseResource(messages.Message):
  question_response_id = messages.StringField(1)
  parent_id = messages.StringField(2)
  questionnaire_response_id = messages.StringField(3)
  ordinal = messages.IntegerField(4)
  linkId = messages.StringField(5)
  text = messages.StringField(6)
  answer = messages.MessageField(AnswerResource, 7, repeated=True)


class QuestionResponse(DataAccessObject):
  def __init__(self):
    super(QuestionResponse, self).__init__(
        resource=QuestionResponseResource,
        table='question_response',
        columns=QUESTION_RESPONSE_COLUMNS,
        key_columns=QUESTION_RESPONSE_KEY_COLUMNS)
    self.set_synthetic_fields(QUESTION_RESPONSE_KEY_COLUMNS
                              + ('parent_id', 'ordinal',))

  def link(self, obj, parent, ordinal):
    obj.parent_id = parent.questionnaire_response_group_id
    obj.questionnaire_response_id = parent.questionnaire_response_id
    obj.ordinal = ordinal
    if not obj.question_response_id:
      obj.question_response_id = str(uuid.uuid4())


QUESTIONNAIRE_RESPONSE_GROUP_KEY_COLUMNS = ('questionnaire_response_group_id',
                                            'questionnaire_response_id')
QUESTIONNAIRE_RESPONSE_GROUP_COLUMNS = \
    QUESTIONNAIRE_RESPONSE_GROUP_KEY_COLUMNS + (
        'parent_id', 'ordinal', 'linkId', 'title', 'text', 'subject')

class QuestionnaireResponseGroupResource(messages.Message):
  questionnaire_response_group_id = messages.StringField(1)
  parent_id = messages.StringField(2)
  questionnaire_response_id = messages.StringField(3)
  ordinal = messages.IntegerField(4)
  linkId = messages.StringField(5)
  title = messages.StringField(6)
  text = messages.StringField(7)
  subject = messages.MessageField(fhir_resources.ReferenceResource, 8,
                                  repeated=False)
  group = messages.MessageField('QuestionnaireResponseGroupResource', 9,
                                repeated=True)
  question = messages.MessageField(QuestionResponseResource, 10, repeated=True)


class QuestionnaireResponseGroup(DataAccessObject):
  def __init__(self):
    super(QuestionnaireResponseGroup, self).__init__(
        resource=QuestionnaireResponseGroupResource,
        table='questionnaire_response_group',
        columns=QUESTIONNAIRE_RESPONSE_GROUP_COLUMNS,
        key_columns=QUESTIONNAIRE_RESPONSE_GROUP_KEY_COLUMNS)
    self.set_synthetic_fields(QUESTIONNAIRE_RESPONSE_GROUP_KEY_COLUMNS
                              + ('parent_id', 'ordinal',))

  def link(self, obj, parent, ordinal):
    if type(parent) == QuestionnaireResponseResource:
      obj.parent_id = parent.id
      obj.questionnaire_response_id = parent.id
    else:
      obj.questionnaire_response_id = parent.questionnaire_response_id
      if type(parent) == AnswerResource:
        obj.parent_id = parent.answer_id
      elif type(parent) == QuestionnaireResponseGroupResource:
        obj.parent_id = parent.questionnaire_response_group_id
    obj.ordinal = ordinal
    if not obj.questionnaire_response_group_id:
      obj.questionnaire_response_group_id = str(uuid.uuid4())

QUESTIONNAIRE_RESPONSE_KEY_COLUMNS = ('id',)
QUESTIONNAIRE_RESPONSE_COLUMNS = QUESTIONNAIRE_RESPONSE_KEY_COLUMNS + (
    'resourceType',
    'meta',
    'implicitRules',
    'language',
    'text',
    'contained',
    'identifier',
    'questionnaire',
    'status',
    'subject',
    'author',
    'authored',
    'source',
    'encounter')

class QuestionnaireResponseResource(messages.Message):
  resourceType = messages.StringField(1)
  id = messages.StringField(2)
  meta = messages.MessageField(fhir_resources.MetaResource, 3, repeated=False)
  implicitRules = messages.StringField(4)
  language = messages.StringField(5)
  text = messages.MessageField(fhir_resources.NarrativeResource, 6,
                               repeated=False)
  contained = messages.MessageField(fhir_resources.DomainUsageResourceResource,
                                    7, repeated=True)
  identifier = messages.MessageField(fhir_resources.IdentifierResource, 8,
                                     repeated=True)
  questionnaire = messages.MessageField(fhir_resources.ReferenceResource, 9,
                                        repeated=False)
  status = messages.StringField(10)
  subject = messages.MessageField(fhir_resources.ReferenceResource, 11,
                                  repeated=False)
  author = messages.MessageField(fhir_resources.ReferenceResource, 12,
                                 repeated=False)
  authored = message_types.DateTimeField(13)
  source = messages.MessageField(fhir_resources.ReferenceResource, 14,
                                 repeated=False)
  encounter = messages.MessageField(fhir_resources.ReferenceResource, 15,
                                    repeated=False)
  group = messages.MessageField(QuestionnaireResponseGroupResource, 16,
                                repeated=False)


class QuestionnaireResponse(DataAccessObject):
  def __init__(self):
    super(QuestionnaireResponse, self).__init__(
        resource=QuestionnaireResponseResource,
        table='questionnaire_response',
        columns=QUESTIONNAIRE_RESPONSE_COLUMNS,
        key_columns=QUESTIONNAIRE_RESPONSE_KEY_COLUMNS)

  def assemble(self, questionnaire_response):
    qid = questionnaire_response.id
    # Request_obj here should have the questionnaire id set in the field 'id'.
    question_responses = QUESTION_RESPONSE_DAO.list(
        QuestionResponseResource(questionnaire_response_id=qid))
    groups = QUESTIONNAIRE_RESPONSE_GROUP_DAO.list(
        QuestionnaireResponseGroupResource(questionnaire_response_id=qid))
    answers = ANSWER_DAO.list(AnswerResource(questionnaire_response_id=qid))

    parent_to_question_responses = collections.defaultdict(list)
    parent_to_groups = collections.defaultdict(list)
    parent_to_answers = collections.defaultdict(list)

    for r in question_responses:
      parent_to_question_responses[r.parent_id].append(r)

    for group in groups:
      parent_to_groups[group.parent_id].append(group)

    for answer in answers:
      parent_to_answers[answer.parent_id].append(answer)

    # Questionnaire_responses have a single group.
    top_groups = parent_to_groups[qid]
    if len(top_groups) != 1:
      raise BaseException(
          "Found questionnaire_response with {} groups".format(len(top_groups)))
    questionnaire_response.group = top_groups[0]

    # QuestionnaireResponseGroups may contain multiple QuestionnaireGroups and
    # multiple QuestionResponses.
    for group in groups:
      group.group = sorted(
          parent_to_groups[group.questionnaire_response_group_id],
          key=lambda g: g.ordinal)
      group.question = sorted(
          parent_to_question_responses[group.questionnaire_response_group_id],
          key=lambda q: q.ordinal)

    # QuestionResponses may contain multiple Answers.
    for r in question_responses:
      r.answer = sorted(parent_to_answers[r.question_response_id],
                         key=lambda a: a.ordinal)

    # Answers may have multiple groups.
    for answer in answers:
      answer.group = sorted(parent_to_groups[answer.answer_id],
                            key=lambda g: g.ordinal)

QUESTIONNAIRE_RESPONSE_GROUP_DAO = QuestionnaireResponseGroup()
QUESTION_RESPONSE_DAO = QuestionResponse()
ANSWER_DAO = Answer()

DAO = QuestionnaireResponse()


QUESTIONNAIRE_RESPONSE_GROUP_DAO.add_child_message(
    'group', QUESTIONNAIRE_RESPONSE_GROUP_DAO)
QUESTIONNAIRE_RESPONSE_GROUP_DAO.add_child_message(
    'question', QUESTION_RESPONSE_DAO)

QUESTION_RESPONSE_DAO.add_child_message('answer', ANSWER_DAO)
ANSWER_DAO.add_child_message('group', QUESTIONNAIRE_RESPONSE_GROUP_DAO)

DAO.add_child_message('group', QUESTIONNAIRE_RESPONSE_GROUP_DAO)
