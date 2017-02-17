import datetime
import test_data

from dao.questionnaire_dao import QuestionnaireDao, QuestionnaireHistoryDao
from dao.questionnaire_dao import QuestionnaireConceptDao, QuestionnaireQuestionDao
from model.questionnaire import Questionnaire, QuestionnaireHistory
from model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from unit_test_util import SqlTestBase
from clock import FakeClock
from werkzeug.exceptions import BadRequest

class QuestionnaireDaoTest(SqlTestBase):
  def setUp(self):
    super(QuestionnaireDaoTest, self).setUp()
    self.dao = QuestionnaireDao()
    self.questionnaire_history_dao = QuestionnaireHistoryDao()
    self.questionnaire_concept_dao = QuestionnaireConceptDao()
    self.questionnaire_question_dao = QuestionnaireQuestionDao()

  def test_get_before_insert(self):
    self.assertFalse(self.dao.get(1))
    self.assertFalse(self.dao.get_with_children(1))
    self.assertFalse(self.questionnaire_history_dao.get([1, 1]))
    self.assertFalse(self.questionnaire_history_dao.get_with_children([1, 1]))
    self.assertFalse(self.questionnaire_concept_dao.get(1))
    self.assertFalse(self.questionnaire_question_dao.get(1))

  def test_insert(self):
    q = Questionnaire(resource='blah')
    q.concepts.append(QuestionnaireConcept(conceptSystem='a', conceptCode='b'))
    q.concepts.append(QuestionnaireConcept(conceptSystem='c', conceptCode='d'))
    q.questions.append(QuestionnaireQuestion(linkId='a', conceptSystem='b', conceptCode='c'))
    q.questions.append(QuestionnaireQuestion(linkId='d', conceptSystem='e', conceptCode='f'))
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.dao.insert(q)

    expected_questionnaire = Questionnaire(questionnaireId=1, version=1, created=time,
                                          lastModified=time, resource='blah')
    questionnaire = self.dao.get(1)
    self.assertEquals(expected_questionnaire.asdict(), questionnaire.asdict())

    expected_concept_1 = QuestionnaireConcept(questionnaireConceptId=1, questionnaireId=1,
                                              questionnaireVersion=1, conceptSystem='a', conceptCode='b')
    expected_concept_2 = QuestionnaireConcept(questionnaireConceptId=2, questionnaireId=1,
                                              questionnaireVersion=1, conceptSystem='c', conceptCode='d')
    expected_question_1 = QuestionnaireQuestion(questionnaireQuestionId=1, questionnaireId=1,
                                                questionnaireVersion=1, linkId='a', conceptSystem='b',
                                                conceptCode='c')
    expected_question_2 = QuestionnaireQuestion(questionnaireQuestionId=2, questionnaireId=1,
                                                questionnaireVersion=1, linkId='d', conceptSystem='e',
                                                conceptCode='f')
    expected_questionnaire.concepts.append(expected_concept_1)
    expected_questionnaire.concepts.append(expected_concept_2)
    expected_questionnaire.questions.append(expected_question_1)
    expected_questionnaire.questions.append(expected_question_2)

    questionnaire = self.dao.get_with_children(1)
    concepts_and_questions = {'concepts':{}, 'questions':{}}
    self.assertEquals(expected_questionnaire.asdict(follow=concepts_and_questions),
                      questionnaire.asdict(follow=concepts_and_questions))

    # Creating a questionnaire creates a history entry with children
    expected_history = QuestionnaireHistory(questionnaireId=1, version=1, created=time,
                                            lastModified=time, resource='blah')
    questionnaire_history = self.questionnaire_history_dao.get([1, 1])
    self.assertEquals(expected_history.asdict(), questionnaire_history.asdict())

    questionnaire_history = self.questionnaire_history_dao.get_with_children([1, 1])
    expected_history.concepts.append(expected_concept_1)
    expected_history.concepts.append(expected_concept_2)
    expected_history.questions.append(expected_question_1)
    expected_history.questions.append(expected_question_2)

    self.assertEquals(expected_history.asdict(follow=concepts_and_questions),
                      questionnaire_history.asdict(follow=concepts_and_questions))

    self.assertEquals(expected_concept_1, self.questionnaire_concept_dao.get(1))
    self.assertEquals(expected_concept_2, self.questionnaire_concept_dao.get(2))
    self.assertEquals(expected_question_1, self.questionnaire_question_dao.get(1))
    self.assertEquals(expected_question_2, self.questionnaire_question_dao.get(2))
