import datetime
import json

from code_constants import PPI_SYSTEM, GENDER_IDENTITY_QUESTION_CODE, THE_BASICS_PPI_MODULE
from dao.code_dao import CodeDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.questionnaire_dao import QuestionnaireDao
from dao.questionnaire_response_dao import QuestionnaireResponseDao, QuestionnaireResponseAnswerDao
from model.code import Code, CodeType
from model.participant import Participant
from model.questionnaire import Questionnaire, QuestionnaireQuestion, QuestionnaireConcept
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from participant_enums import QuestionnaireStatus, WithdrawalStatus
from test_data import consent_code, first_name_code, last_name_code, email_code
from unit_test_util import FlaskTestBase
from clock import FakeClock
from werkzeug.exceptions import BadRequest, Forbidden
from sqlalchemy.exc import IntegrityError

TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)

ANSWERS = {'answers': {}}
QUESTIONNAIRE_RESOURCE = '{"x": "y"}'
QUESTIONNAIRE_RESOURCE_2 = '{"x": "z"}'
QUESTIONNAIRE_RESPONSE_RESOURCE = '{"a": "b"}'
QUESTIONNAIRE_RESPONSE_RESOURCE_2 = '{"a": "c"}'
QUESTIONNAIRE_RESPONSE_RESOURCE_3 = '{"a": "d"}'

def with_id(resource, id_):
  resource_json = json.loads(resource)
  resource_json['id'] = str(id_)
  return json.dumps(resource_json)

class QuestionnaireResponseDaoTest(FlaskTestBase):
  def setUp(self):
    super(QuestionnaireResponseDaoTest, self).setUp()
    self.code_dao = CodeDao()
    self.participant_dao = ParticipantDao()
    self.questionnaire_dao = QuestionnaireDao()
    self.questionnaire_response_dao = QuestionnaireResponseDao()
    self.questionnaire_response_answer_dao = QuestionnaireResponseAnswerDao()
    self.participant_summary_dao = ParticipantSummaryDao()
    self.CODE_1 = Code(codeId=1, system=PPI_SYSTEM, value=GENDER_IDENTITY_QUESTION_CODE,
                       display=u'c', topic=u'd', codeType=CodeType.QUESTION, mapped=True)
    self.CODE_2 = Code(codeId=2, system='a', value='x', display=u'y', codeType=CodeType.QUESTION,
                       mapped=False)
    self.CODE_3 = Code(codeId=3, system='a', value='c', codeType=CodeType.ANSWER, mapped=True,
                       parentId=1)
    self.CODE_4 = Code(codeId=4, system='a', value='d', codeType=CodeType.ANSWER, mapped=True,
                       parentId=2)
    self.CODE_5 = Code(codeId=5, system='a', value='e', codeType=CodeType.ANSWER, mapped=False,
                       parentId=1)
    self.CODE_6 = Code(codeId=6, system='a', value='f', codeType=CodeType.ANSWER, mapped=True,
                       parentId=1)
    self.MODULE_CODE_7 = Code(codeId=7, system=PPI_SYSTEM, value=THE_BASICS_PPI_MODULE,
                              codeType=CodeType.MODULE, mapped=True)
    self.CONCEPT_1 = QuestionnaireConcept(codeId=7)
    self.CODE_1_QUESTION_1 = QuestionnaireQuestion(linkId='a', codeId=1, repeats=False)
    self.CODE_2_QUESTION = QuestionnaireQuestion(linkId='d', codeId=2, repeats=True)
    # Same code as question 1
    self.CODE_1_QUESTION_2 = QuestionnaireQuestion(linkId='x', codeId=1, repeats=False)

  def _names_and_email_answers(self):
    return [self.FN_ANSWER, self.LN_ANSWER, self.EMAIL_ANSWER]

  def test_get_before_insert(self):
    self.assertIsNone(self.questionnaire_response_dao.get(1))
    self.assertIsNone(self.questionnaire_response_dao.get_with_children(1))
    self.assertIsNone(self.questionnaire_response_answer_dao.get(1))

  def test_insert_questionnaire_not_found(self):
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_participant_not_found(self):
    self.insert_codes()
    q = Questionnaire(resource=QUESTIONNAIRE_RESOURCE)
    q.concepts.append(QuestionnaireConcept(codeId=self.consent_code_id))
    self.questionnaire_dao.insert(q)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.extend(self._names_and_email_answers())
    # Answers are there but the participant is not.
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_participant_withdrawn(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2, withdrawalStatus=WithdrawalStatus.NO_USE)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.extend(self._names_and_email_answers())
    with self.assertRaises(Forbidden):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_not_name_answers(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.append(QuestionnaireResponseAnswer(questionnaireResponseAnswerId=2,
                                           questionnaireResponseId=1,
                                           questionId=2, valueSystem='c', valueCodeId=4))
    # Both first and last name are required.
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_first_name_only(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.append(self.FN_ANSWER)
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_last_name_only(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.append(self.LN_ANSWER)
    # Both first and last name are required.
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_names_only(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.append(self.FN_ANSWER)
    qr.answers.append(self.LN_ANSWER)
    # Email is required.
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_email_only(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.append(self.EMAIL_ANSWER)
    # First and last name are required.
    with self.assertRaises(BadRequest):
      self.questionnaire_response_dao.insert(qr)

  def test_insert_both_names_and_email(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.extend(self._names_and_email_answers())
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.questionnaire_response_dao.insert(qr)

    expected_qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1,
                                        questionnaireVersion=1, participantId=1,
                                        resource=with_id(QUESTIONNAIRE_RESPONSE_RESOURCE, 1),
                                        created=time)
    expected_qr.answers.extend(self._names_and_email_answers())
    qr2 = self.questionnaire_response_dao.get(1)
    self.assertEquals(expected_qr.asdict(), qr2.asdict())
    self.check_response(expected_qr)

  def test_insert_duplicate(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.extend(self._names_and_email_answers())
    self.questionnaire_response_dao.insert(qr)
    qr2 = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                                participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE_2)
    with self.assertRaises(IntegrityError):
      self.questionnaire_response_dao.insert(qr2)

  def test_get_after_withdrawal_fails(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    qr.answers.extend(self._names_and_email_answers())
    self.questionnaire_response_dao.insert(qr)
    p.withdrawalStatus = WithdrawalStatus.NO_USE
    self.participant_dao.update(p)
    with self.assertRaises(Forbidden):
      self.questionnaire_response_dao.get(qr.questionnaireResponseId)

  def check_response(self, expected_qr):
    qr = self.questionnaire_response_dao.get_with_children(expected_qr.questionnaireResponseId)
    self.assertEquals(expected_qr.asdict(follow=ANSWERS), qr.asdict(follow=ANSWERS))

  def insert_codes(self):
    self.code_dao.insert(self.CODE_1)
    self.code_dao.insert(self.CODE_2)
    self.code_dao.insert(self.CODE_3)
    self.code_dao.insert(self.CODE_4)
    self.code_dao.insert(self.CODE_5)
    self.code_dao.insert(self.CODE_6)
    self.code_dao.insert(self.MODULE_CODE_7)
    self.consent_code_id = self.code_dao.insert(consent_code()).codeId
    self.first_name_code_id = self.code_dao.insert(first_name_code()).codeId
    self.last_name_code_id = self.code_dao.insert(last_name_code()).codeId
    self.email_code_id = self.code_dao.insert(email_code()).codeId
    self.FN_QUESTION = QuestionnaireQuestion(linkId='fn', codeId=self.first_name_code_id,
                                             repeats=False)
    self.LN_QUESTION = QuestionnaireQuestion(linkId='ln', codeId=self.last_name_code_id,
                                             repeats=False)
    self.EMAIL_QUESTION = QuestionnaireQuestion(linkId='email', codeId=self.email_code_id,
                                                repeats=False)
    self.first_name = self.fake.first_name()
    self.last_name = self.fake.last_name()
    self.email = self.fake.email()
    self.FN_ANSWER = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=3,
                                                 questionnaireResponseId=1,
                                                 questionId=3, valueString=self.first_name)
    self.LN_ANSWER = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=4,
                                                 questionnaireResponseId=1,
                                                 questionId=4, valueString=self.last_name)
    self.EMAIL_ANSWER = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=5,
                                                    questionnaireResponseId=1,
                                                    questionId=5, valueString=self.email)


  def _setup_questionnaire(self):
    q = Questionnaire(resource=QUESTIONNAIRE_RESOURCE)
    q.concepts.append(self.CONCEPT_1)
    q.concepts.append(QuestionnaireConcept(codeId=self.consent_code_id))
    q.questions.append(self.CODE_1_QUESTION_1)
    q.questions.append(self.CODE_2_QUESTION)
    q.questions.append(self.FN_QUESTION)
    q.questions.append(self.LN_QUESTION)
    q.questions.append(self.EMAIL_QUESTION)
    self.questionnaire_dao.insert(q)

  def test_insert_with_answers(self):
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    with FakeClock(TIME):
      self.participant_dao.insert(p)
    self._setup_questionnaire()
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    answer_1 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=1,
                                           questionnaireResponseId=1,
                                           questionId=1, valueSystem='a', valueCodeId=3,
                                           valueDecimal=123, valueString=self.fake.first_name(),
                                           valueDate=datetime.date.today())
    answer_2 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=2,
                                           questionnaireResponseId=1,
                                           questionId=2, valueSystem='c', valueCodeId=4)
    qr.answers.append(answer_1)
    qr.answers.append(answer_2)
    names_and_email_answers = self._names_and_email_answers()
    qr.answers.extend(names_and_email_answers)
    with FakeClock(TIME_2):
      self.questionnaire_response_dao.insert(qr)

    expected_qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1,
                                        questionnaireVersion=1, participantId=1,
                                        resource=with_id(QUESTIONNAIRE_RESPONSE_RESOURCE, 1),
                                        created=TIME_2)
    qr2 = self.questionnaire_response_dao.get(1)
    self.assertEquals(expected_qr.asdict(), qr2.asdict())

    expected_qr.answers.append(answer_1)
    expected_qr.answers.append(answer_2)
    expected_qr.answers.extend(names_and_email_answers)
    self.check_response(expected_qr)

    expected_ps = self._participant_summary_with_defaults(
        participantId=1, biobankId=2, genderIdentityId=3, signUpTime=TIME,
        numCompletedBaselinePPIModules=1, numCompletedPPIModules=1,
        questionnaireOnTheBasics=QuestionnaireStatus.SUBMITTED,
        questionnaireOnTheBasicsTime=TIME_2,
        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
        consentForStudyEnrollmentTime=TIME_2,
        firstName=self.first_name, lastName=self.last_name, email=self.email)
    self.assertEquals(expected_ps.asdict(), self.participant_summary_dao.get(1).asdict())

  def test_insert_qr_three_times(self):
    """Adds three questionnaire responses for the same participant.

    The latter two responses are for the same questionnaire, answering a question that has the
    same concept code and system as a question found on the first (different) questionnaire.

    Verifies that new answers set endTime on answers for questions with the same concept for the
    same participant, whether on the same questionnaire or a different questionnaire,
    without affecting other answers.
    """
    self.insert_codes()
    p = Participant(participantId=1, biobankId=2)
    with FakeClock(TIME):
      self.participant_dao.insert(p)
    self._setup_questionnaire()
    q2 = Questionnaire(resource=QUESTIONNAIRE_RESOURCE_2)
    # The question on the second questionnaire has the same concept as the first question on the
    # first questionnaire; answers to it will thus set endTime for answers to the first question.
    q2.questions.append(self.CODE_1_QUESTION_2)

    self.questionnaire_dao.insert(q2)

    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource=QUESTIONNAIRE_RESPONSE_RESOURCE)
    answer_1 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=1,
                                           questionnaireResponseId=1,
                                           questionId=1, valueSystem='a', valueCodeId=3,
                                           valueDecimal=123, valueString=self.fake.first_name(),
                                           valueDate=datetime.date.today())
    answer_2 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=2,
                                           questionnaireResponseId=1,
                                           questionId=2, valueSystem='c', valueCodeId=4)
    qr.answers.append(answer_1)
    qr.answers.append(answer_2)
    qr.answers.extend(self._names_and_email_answers())
    with FakeClock(TIME_2):
      self.questionnaire_response_dao.insert(qr)

    expected_ps = self._participant_summary_with_defaults(
        participantId=1, biobankId=2, genderIdentityId=3, signUpTime=TIME,
        numCompletedBaselinePPIModules=1, numCompletedPPIModules=1,
        questionnaireOnTheBasics=QuestionnaireStatus.SUBMITTED,
        questionnaireOnTheBasicsTime=TIME_2,
        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
        consentForStudyEnrollmentTime=TIME_2,
        firstName=self.first_name, lastName=self.last_name, email=self.email)
    self.assertEquals(expected_ps.asdict(), self.participant_summary_dao.get(1).asdict())

    qr2 = QuestionnaireResponse(questionnaireResponseId=2, questionnaireId=2,
                                questionnaireVersion=1, participantId=1,
                                resource=QUESTIONNAIRE_RESPONSE_RESOURCE_2)
    answer_3 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=6,
                                           questionnaireResponseId=2,
                                           questionId=6, valueSystem='x', valueCodeId=5,
                                           valueDecimal=123, valueString=self.fake.last_name(),
                                           valueDate=datetime.date.today())
    qr2.answers.append(answer_3)
    with FakeClock(TIME_3):
      self.questionnaire_response_dao.insert(qr2)

    expected_qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1,
                                        questionnaireVersion=1, participantId=1,
                                        resource=with_id(QUESTIONNAIRE_RESPONSE_RESOURCE, 1),
                                        created=TIME_2)
    # Answer one on the original response should be marked as ended, since a question with
    # the same concept was answered. Answer two should be left alone.
    answer_1.endTime = TIME_3
    expected_qr.answers.append(answer_1)
    expected_qr.answers.append(answer_2)
    expected_qr.answers.extend(self._names_and_email_answers())
    self.check_response(expected_qr)

    # The new questionnaire response should be there, too.
    expected_qr2 = QuestionnaireResponse(questionnaireResponseId=2, questionnaireId=2,
                                         questionnaireVersion=1, participantId=1,
                                         resource=with_id(QUESTIONNAIRE_RESPONSE_RESOURCE_2, 2),
                                         created=TIME_3)
    expected_qr2.answers.append(answer_3)
    self.check_response(expected_qr2)

    expected_ps2 = self._participant_summary_with_defaults(
        participantId=1, biobankId=2, genderIdentityId=5, signUpTime=TIME,
        numCompletedBaselinePPIModules=1, numCompletedPPIModules=1,
        questionnaireOnTheBasics=QuestionnaireStatus.SUBMITTED,
        questionnaireOnTheBasicsTime=TIME_2,
        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
        consentForStudyEnrollmentTime=TIME_2,
        firstName=self.first_name, lastName=self.last_name, email=self.email)
    # The participant summary should be updated with the new gender identity, but nothing else
    # changes.
    self.assertEquals(expected_ps2.asdict(), self.participant_summary_dao.get(1).asdict())

    qr3 = QuestionnaireResponse(questionnaireResponseId=3, questionnaireId=2,
                                questionnaireVersion=1, participantId=1,
                                resource=QUESTIONNAIRE_RESPONSE_RESOURCE_3)
    answer_4 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=7,
                                           questionnaireResponseId=3,
                                           questionId=6, valueSystem='z', valueCodeId=6,
                                           valueDecimal=456, valueString=self.fake.last_name(),
                                           valueDate=datetime.date.today())
    qr3.answers.append(answer_4)
    with FakeClock(TIME_4):
      self.questionnaire_response_dao.insert(qr3)

    # The first questionnaire response hasn't changed.
    self.check_response(expected_qr)

    # The second questionnaire response's answer should have had an endTime set.
    answer_3.endTime = TIME_4
    self.check_response(expected_qr2)

    # The third questionnaire response should be there.
    expected_qr3 = QuestionnaireResponse(questionnaireResponseId=3, questionnaireId=2,
                                        questionnaireVersion=1, participantId=1,
                                        resource=with_id(QUESTIONNAIRE_RESPONSE_RESOURCE_3, 3),
                                        created=TIME_4)
    expected_qr3.answers.append(answer_4)
    self.check_response(expected_qr3)

    expected_ps3 = self._participant_summary_with_defaults(
        participantId=1, biobankId=2, genderIdentityId=6, signUpTime=TIME,
        numCompletedBaselinePPIModules=1, numCompletedPPIModules=1,
        questionnaireOnTheBasics=QuestionnaireStatus.SUBMITTED,
        questionnaireOnTheBasicsTime=TIME_2,
        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
        consentForStudyEnrollmentTime=TIME_2,
        firstName=self.first_name, lastName=self.last_name, email=self.email)
    # The participant summary should be updated with the new gender identity, but nothing else
    # changes.
    self.assertEquals(expected_ps3.asdict(), self.participant_summary_dao.get(1).asdict())
