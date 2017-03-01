import json

from model.code import CodeType
from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Date, DateTime, BLOB, ForeignKey, String, ForeignKeyConstraint  # pylint: disable=line-too-long
import fhirclient.models.questionnaireresponse
from werkzeug.exceptions import BadRequest

_QUESTIONNAIRE_PREFIX = 'Questionnaire/'
_QUESTIONNAIRE_HISTORY_SEGMENT = '/_history/'
_QUESTIONNAIRE_REFERENCE_FORMAT = (_QUESTIONNAIRE_PREFIX + "%d" +
                                   _QUESTIONNAIRE_HISTORY_SEGMENT + "%d")

class QuestionnaireResponse(Base):
  """"A response to a questionnaire for a participant. Contains answers to questions found in the
  questionnaire."""
  __tablename__ = 'questionnaire_response'
  questionnaireResponseId = Column('questionnaire_response_id', Integer, primary_key=True, 
                                   autoincrement=False)
  questionnaireId = Column('questionnaire_id', Integer, nullable=False)
  questionnaireVersion = Column('questionnaire_version', Integer, nullable=False)  
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'), 
                         nullable=False)
  created = Column('created', DateTime, nullable=False)
  resource = Column('resource', BLOB, nullable=False)
  answers = relationship('QuestionnaireResponseAnswer', cascade='all, delete-orphan')
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], 
                         ['questionnaire_history.questionnaire_id', 
                          'questionnaire_history.version']),
  )

  def to_client_json(self):
    return json.loads(self.resource)

  @staticmethod
  def from_client_json(resource_json, participant_id=None, id_=None,
                       expected_version=None, client_id=None):
    fhir_qr = fhirclient.models.questionnaireresponse.QuestionnaireResponse(resource_json)
    patient_id = fhir_qr.subject.reference
    if (patient_id != 'Patient/P{}'.format(participant_id)):
      raise BadRequest("Questionnaire response subject reference does not match participant_id %d"
                       % patient_id)
    questionnaire = QuestionnaireResponse._get_questionnaire(fhir_qr.questionnaire)
    qr = QuestionnaireResponse(questionnaireId=questionnaire.questionnaireId,
                               questionnaireVersion=questionnaire.version,
                               participantId=participant_id,
                               resource=json.dumps(fhir_qr.as_json()))

    # Extract a code map and answers from the questionnaire response.
    code_map, answers = QuestionnaireResponse._extract_codes_and_answers(fhir_qr.group,
                                                                         questionnaire)
    from dao.code_dao import CodeDao
    # Get or insert codes, and retrieve their database IDs.
    code_id_map = CodeDao().get_or_add_codes(code_map)

     # Now add the child answers, using the IDs in code_id_map
    QuestionnaireResponse._add_answers(qr, code_id_map, answers)

    return qr

  @staticmethod
  def _get_questionnaire(questionnaire):
    """Retrieves the questionnaire referenced by this response; mutates the reference to include
    the version if it doesn't already."""
    if not questionnaire.reference.startswith(_QUESTIONNAIRE_PREFIX):
      raise BadRequest('Questionnaire reference %s is invalid' % questionnaire.reference)
    from dao.questionnaire_dao import QuestionnaireHistoryDao
    questionnaire_reference = questionnaire.reference[len(_QUESTIONNAIRE_PREFIX):]
    # If the questionnaire response specifies the version of the questionnaire it's for, use it.
    if _QUESTIONNAIRE_HISTORY_SEGMENT in questionnaire_reference:
      questionnaire_ref_parts = questionnaire_reference.split(_QUESTIONNAIRE_HISTORY_SEGMENT)
      if len(questionnaire_ref_parts) != 2:
        raise BadRequest('Questionnaire id %s is invalid' % questionnaire_reference)
      try:
        questionnaire_id = int(questionnaire_ref_parts[0])
        version = int(questionnaire_ref_parts[1])
        q = QuestionnaireHistoryDao().get_with_children((questionnaire_id, version))
        if not q:
          raise BadRequest('Questionnaire with id %d, version %d is not found' %
                           (questionnaire_id, version))
        return q
      except ValueError:
        raise BadRequest('Questionnaire id %s is invalid' % questionnaire_reference)
    else:
      try:
        questionnaire_id = int(questionnaire_reference)
        from dao.questionnaire_dao import QuestionnaireDao
        q = QuestionnaireDao().get_with_children(questionnaire_id)
        if not q:
          raise BadRequest('Questionnaire with id %d is not found' % questionnaire_id)
        # Mutate the questionnaire reference to include the version.
        questionnaire.reference = _QUESTIONNAIRE_REFERENCE_FORMAT % (questionnaire_id, q.version)
        return q
      except ValueError:
        raise BadRequest('Questionnaire id %s is invalid' % questionnaire_reference)

  @staticmethod
  def _extract_codes_and_answers(group, q):
    """Returns (system, code) -> (display, code type, question code id) code map
    and (QuestionnaireResponseAnswer, (system, code)) answer pairs."""
    code_map = {}
    answers = []
    link_id_to_question = {}
    if q.questions:
      link_id_to_question = {question.linkId: question for question in q.questions}
    QuestionnaireResponse._populate_codes_and_answers(group, code_map, answers, link_id_to_question,
                                                      q.questionnaireId)
    return (code_map, answers)

  @staticmethod
  def _populate_codes_and_answers(group, code_map, answers, link_id_to_question,
                                  questionnaire_id):
    """Populates code_map with (system, code) -> (display, code type, question code id)
    and answers with (QuestionnaireResponseAnswer, (system, code)) pairs."""
    if group.question:
      for question in group.question:
        if question.linkId and question.answer:
          qq = link_id_to_question.get(question.linkId)
          if qq:
            for answer in question.answer:
              qr_answer = QuestionnaireResponseAnswer(questionId=qq.questionnaireQuestionId)
              system_and_code = None
              if answer.valueCoding:
                system_and_code = (answer.valueCoding.system, answer.valueCoding.code)
                if not system_and_code in code_map:
                  code_map[system_and_code] = (answer.valueCoding.display, CodeType.ANSWER, qq.codeId)
              if answer.valueDecimal:
                qr_answer.valueDecimal = int(answer.valueDecimal)
              if answer.valueString:
                qr_answer.valueString = answer.valueString
              if answer.valueDate:
                qr_answer.valueDate = answer.valueDate.date
              answers.append((qr_answer, system_and_code))
              if answer.group:
                for sub_group in answer.group:
                  QuestionnaireResponse._populate_codes_and_answers(sub_group, code_map, answers,
                                                                    link_id_to_question,
                                                                    questionnaire_id)

    if group.group:
      for sub_group in group.group:
        QuestionnaireResponse._populate_codes_and_answers(sub_group, code_map, answers,
                                                          link_id_to_question, questionnaire_id)
  @staticmethod
  def _add_answers(qr, code_id_map, answers):
    for answer, system_and_code in answers:
      answer.valueCodeId = code_id_map[system_and_code]
      qr.answers.append(answer)


class QuestionnaireResponseAnswer(Base):
  """An answer found in a questionnaire response. Note that there could be multiple answers to 
  the same question, if the questionnaire allows for multiple answers.

  An answer is given to a particular question which has a particular concept code. The answer is
  the current answer for a participant from the time period between its parent response's creation
  field and the endTime field (or now, if endTime is not set.)

  When an answer is given by a participant in a questionnaire response, the endTime of any previous
  answers to questions with the same concept codes that don't have endTime set yet should have
  endTime set to the current time.
  """
  __tablename__ = 'questionnaire_response_answer'
  questionnaireResponseAnswerId = Column('questionnaire_response_answer_id', Integer,
                                         primary_key=True)
  questionnaireResponseId = Column('questionnaire_response_id', Integer, 
      ForeignKey('questionnaire_response.questionnaire_response_id'), nullable=False)
  questionId = Column('question_id', Integer, 
                      ForeignKey('questionnaire_question.questionnaire_question_id'), 
                      nullable=False)
  # The time at which this answer was replaced by another answer. Not set if this answer is the
  # latest answer to the question.
  endTime = Column('end_time', DateTime)
  valueSystem = Column('value_system', String(50))
  valueCodeId = Column('value_code_id', Integer, ForeignKey('code.code_id'))
  valueDecimal = Column('value_decimal', Integer)
  # Is this big enough?
  valueString = Column('value_string', String(1024))
  valueDate = Column('value_date', Date)
