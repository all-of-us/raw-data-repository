import datetime
import test_data

from dao.participant_dao import ParticipantDao
from dao.questionnaire_dao import QuestionnaireDao
from dao.questionnaire_response_dao import QuestionnaireResponseDao, QuestionnaireResponseAnswerDao
from model.participant import Participant
from model.questionnaire import Questionnaire, QuestionnaireQuestion
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from unit_test_util import SqlTestBase, sort_lists
from clock import FakeClock
from werkzeug.exceptions import NotFound, PreconditionFailed
from sqlalchemy.exc import IntegrityError

QUESTION_1 = QuestionnaireQuestion(linkId='a', conceptSystem='b', conceptCode='c')
QUESTION_2 = QuestionnaireQuestion(linkId='d', conceptSystem='e', conceptCode='f')
# Same concept as question 1
QUESTION_3 = QuestionnaireQuestion(linkId='x', conceptSystem='b', conceptCode='c')

TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)

ANSWERS = {'answers': {}}

class QuestionnaireResponseDaoTest(SqlTestBase):
  def setUp(self):
    super(QuestionnaireResponseDaoTest, self).setUp()
    self.setup_data()
    self.participant_dao = ParticipantDao()
    self.questionnaire_dao = QuestionnaireDao()
    self.questionnaire_response_dao = QuestionnaireResponseDao()
    self.questionnaire_response_answer_dao = QuestionnaireResponseAnswerDao()

  def test_get_before_insert(self):
    self.assertIsNone(self.questionnaire_response_dao.get(1))
    self.assertIsNone(self.questionnaire_response_dao.get_with_children(1))
    self.assertIsNone(self.questionnaire_response_answer_dao.get(1))

  def test_insert_questionnaire_not_found(self):
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource='blah')
    try:
      self.questionnaire_response_dao.insert(qr)
      fail("IntegrityError expected")
    except IntegrityError:
      pass

  def test_insert_participant_not_found(self):
    q = Questionnaire(resource='blah')
    self.questionnaire_dao.insert(q)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource='blah')
    try:
      self.questionnaire_response_dao.insert(qr)
      fail("IntegrityError expected")
    except IntegrityError:
      pass

  def test_insert_no_answers(self):
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    q = Questionnaire(resource='blah')
    self.questionnaire_dao.insert(q)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource='blah')
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.questionnaire_response_dao.insert(qr)

    expected_qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1,
                                        questionnaireVersion=1, participantId=1,
                                        resource='blah', created=time)
    qr2 = self.questionnaire_response_dao.get(1)
    self.assertEquals(expected_qr.asdict(), qr2.asdict())

    qr3 = self.questionnaire_response_dao.get_with_children(1)
    self.assertEquals([], qr3.answers)

  def test_insert_duplicate(self):
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    q = Questionnaire(resource='blah')
    self.questionnaire_dao.insert(q)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource='blah')
    self.questionnaire_response_dao.insert(qr)
    qr2 = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                                participantId=1, resource='xxx')
    try:
      self.questionnaire_response_dao.insert(qr2)
      self.fail("IntegrityError expected")
    except IntegrityError:
      pass

  def test_insert_with_answers(self):
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    q = Questionnaire(resource='blah')
    q.questions.append(QUESTION_1)
    q.questions.append(QUESTION_2)
    self.questionnaire_dao.insert(q)
    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource='blah')
    answer_1 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=1,
                                           questionnaireResponseId=1,
                                           questionId=1, valueSystem='a', valueCode='b',
                                           valueDecimal=123, valueString='c',
                                           valueDate=datetime.date.today())
    answer_2 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=2,
                                           questionnaireResponseId=1,
                                           questionId=2, valueSystem='c', valueCode='d')
    qr.answers.append(answer_1)
    qr.answers.append(answer_2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.questionnaire_response_dao.insert(qr)

    expected_qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1,
                                        questionnaireVersion=1, participantId=1,
                                        resource='blah', created=time)
    qr2 = self.questionnaire_response_dao.get(1)
    self.assertEquals(expected_qr.asdict(), qr2.asdict())

    expected_qr.answers.append(answer_1)
    expected_qr.answers.append(answer_2)

    qr3 = self.questionnaire_response_dao.get_with_children(1)
    self.assertEquals(expected_qr.asdict(follow=ANSWERS), qr3.asdict(follow=ANSWERS))


  def test_insert_same_questionnaire_three_times(self):
    p = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(p)
    q = Questionnaire(resource='blah')
    q.questions.append(QUESTION_1)
    q.questions.append(QUESTION_2)
    self.questionnaire_dao.insert(q)

    q2 = Questionnaire(resource='blarg')
    q2.questions.append(QUESTION_3)
    self.questionnaire_dao.insert(q2)

    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, resource='blah')
    answer_1 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=1,
                                           questionnaireResponseId=1,
                                           questionId=1, valueSystem='a', valueCode='b',
                                           valueDecimal=123, valueString='c',
                                           valueDate=datetime.date.today())
    answer_2 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=2,
                                           questionnaireResponseId=1,
                                           questionId=2, valueSystem='c', valueCode='d')
    qr.answers.append(answer_1)
    qr.answers.append(answer_2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.questionnaire_response_dao.insert(qr)

    qr2 = QuestionnaireResponse(questionnaireResponseId=2, questionnaireId=2,
                                questionnaireVersion=1, participantId=1, resource='foo')
    answer_3 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=3,
                                           questionnaireResponseId=2,
                                           questionId=3, valueSystem='x', valueCode='y',
                                           valueDecimal=123, valueString='z',
                                           valueDate=datetime.date.today())
    qr2.answers.append(answer_3)
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      self.questionnaire_response_dao.insert(qr2)

    expected_qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1,
                                        questionnaireVersion=1, participantId=1,
                                        resource='blah', created=time)
    # Answer one on the original response should be marked as ended, since a question with
    # the same concept was answered. Answer two should be left alone.
    answer_1.endTime = time2
    expected_qr.answers.append(answer_1)
    expected_qr.answers.append(answer_2)

    qr = self.questionnaire_response_dao.get_with_children(1)
    self.assertEquals(expected_qr.asdict(follow=ANSWERS), qr.asdict(follow=ANSWERS))

    # The new questionnaire response should be there, too.
    expected_qr2 = QuestionnaireResponse(questionnaireResponseId=2, questionnaireId=2,
                                         questionnaireVersion=1, participantId=1,
                                         resource='foo', created=time2)
    expected_qr2.answers.append(answer_3)
    qr2 = self.questionnaire_response_dao.get_with_children(2)
    self.assertEquals(expected_qr2.asdict(follow=ANSWERS), qr2.asdict(follow=ANSWERS))

    qr3 = QuestionnaireResponse(questionnaireResponseId=3, questionnaireId=2,
                                questionnaireVersion=1, participantId=1, resource='zzz')
    answer_4 = QuestionnaireResponseAnswer(questionnaireResponseAnswerId=4,
                                           questionnaireResponseId=3,
                                           questionId=3, valueSystem='z', valueCode='q',
                                           valueDecimal=456, valueString='v',
                                           valueDate=datetime.date.today())
    qr3.answers.append(answer_4)
    time3 = datetime.datetime(2016, 1, 3)
    with FakeClock(time3):
      self.questionnaire_response_dao.insert(qr3)

    # The first questionnaire response hasn't changed.
    qr = self.questionnaire_response_dao.get_with_children(1)
    self.assertEquals(expected_qr.asdict(follow=ANSWERS), qr.asdict(follow=ANSWERS))

    # The second questionnaire response's answer should have had an endTime set.
    answer_3.endTime = time3
    qr2 = self.questionnaire_response_dao.get_with_children(2)
    self.assertEquals(expected_qr2.asdict(follow=ANSWERS), qr2.asdict(follow=ANSWERS))

    # The third questionnaire response should be there.
    expected_qr3 = QuestionnaireResponse(questionnaireResponseId=3, questionnaireId=2,
                                        questionnaireVersion=1, participantId=1,
                                        resource='zzz', created=time3)
    expected_qr3.answers.append(answer_4)
    qr3 = self.questionnaire_response_dao.get_with_children(3)
    self.assertEquals(expected_qr3.asdict(follow=ANSWERS), qr3.asdict(follow=ANSWERS))

