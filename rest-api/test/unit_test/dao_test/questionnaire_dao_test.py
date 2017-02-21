import datetime
import test_data

from dao.questionnaire_dao import QuestionnaireDao, QuestionnaireHistoryDao
from dao.questionnaire_dao import QuestionnaireConceptDao, QuestionnaireQuestionDao
from model.questionnaire import Questionnaire, QuestionnaireHistory
from model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from unit_test_util import SqlTestBase, sort_lists
from clock import FakeClock
from werkzeug.exceptions import BadRequest, NotFound, PreconditionFailed
from sqlalchemy.exc import IntegrityError

CONCEPT_1 = QuestionnaireConcept(conceptSystem='a', conceptCode='b')
CONCEPT_2 = QuestionnaireConcept(conceptSystem='c', conceptCode='d')
QUESTION_1 = QuestionnaireQuestion(linkId='a', conceptSystem='b', conceptCode='c')
QUESTION_2 = QuestionnaireQuestion(linkId='d', conceptSystem='e', conceptCode='f')

EXPECTED_CONCEPT_1 = QuestionnaireConcept(questionnaireConceptId=1, questionnaireId=1,
                                          questionnaireVersion=1, conceptSystem='a', 
                                          conceptCode='b')
EXPECTED_CONCEPT_2 = QuestionnaireConcept(questionnaireConceptId=2, questionnaireId=1,
                                          questionnaireVersion=1, conceptSystem='c', 
                                          conceptCode='d')
EXPECTED_QUESTION_1 = QuestionnaireQuestion(questionnaireQuestionId=1, questionnaireId=1,
                                            questionnaireVersion=1, linkId='a', conceptSystem='b',
                                            conceptCode='c')
EXPECTED_QUESTION_2 = QuestionnaireQuestion(questionnaireQuestionId=2, questionnaireId=1,
                                            questionnaireVersion=1, linkId='d', conceptSystem='e',
                                            conceptCode='f')
TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
CONCEPTS_AND_QUESTIONS = {'concepts':{}, 'questions':{}}

class QuestionnaireDaoTest(SqlTestBase):
  def setUp(self):
    super(QuestionnaireDaoTest, self).setUp()
    self.dao = QuestionnaireDao()
    self.questionnaire_history_dao = QuestionnaireHistoryDao()
    self.questionnaire_concept_dao = QuestionnaireConceptDao()
    self.questionnaire_question_dao = QuestionnaireQuestionDao()
    
  def test_get_before_insert(self):
    self.assertIsNone(self.dao.get(1))
    self.assertIsNone(self.dao.get_with_children(1))
    self.assertIsNone(self.questionnaire_history_dao.get([1, 1]))
    self.assertIsNone(self.questionnaire_history_dao.get_with_children([1, 1]))
    self.assertIsNone(self.questionnaire_concept_dao.get(1))
    self.assertIsNone(self.questionnaire_question_dao.get(1))

  def check_history(self):
    expected_history = QuestionnaireHistory(questionnaireId=1, version=1, created=TIME,
                                            lastModified=TIME, resource='blah')
    questionnaire_history = self.questionnaire_history_dao.get([1, 1])
    self.assertEquals(expected_history.asdict(), questionnaire_history.asdict())

    questionnaire_history = self.questionnaire_history_dao.get_with_children([1, 1])
    expected_history.concepts.append(EXPECTED_CONCEPT_1)
    expected_history.concepts.append(EXPECTED_CONCEPT_2)
    expected_history.questions.append(EXPECTED_QUESTION_1)
    expected_history.questions.append(EXPECTED_QUESTION_2)

    #self.assertEquals(expected_history.asdict(follow=CONCEPTS_AND_QUESTIONS),
    #                  questionnaire_history.asdict(follow=CONCEPTS_AND_QUESTIONS))

    self.assertEquals(EXPECTED_CONCEPT_1.asdict(), self.questionnaire_concept_dao.get(1).asdict())
    self.assertEquals(EXPECTED_CONCEPT_2.asdict(), self.questionnaire_concept_dao.get(2).asdict())
    self.assertEquals(EXPECTED_QUESTION_1.asdict(), self.questionnaire_question_dao.get(1).asdict())
    self.assertEquals(EXPECTED_QUESTION_2.asdict(), self.questionnaire_question_dao.get(2).asdict())
  
  def test_insert(self):
    q = Questionnaire(resource='blah')
    q.concepts.append(CONCEPT_1)
    q.concepts.append(CONCEPT_2)
    q.questions.append(QUESTION_1)
    q.questions.append(QUESTION_2)
    
    with FakeClock(TIME):
      self.dao.insert(q)
      
    # Creating a questionnaire creates a history entry with children
    self.check_history()

    expected_questionnaire = Questionnaire(questionnaireId=1, version=1, created=TIME,
                                          lastModified=TIME, resource='blah')
    questionnaire = self.dao.get(1)
    self.assertEquals(expected_questionnaire.asdict(), questionnaire.asdict())

    expected_questionnaire.concepts.append(EXPECTED_CONCEPT_1)
    expected_questionnaire.concepts.append(EXPECTED_CONCEPT_2)
    expected_questionnaire.questions.append(EXPECTED_QUESTION_1)
    expected_questionnaire.questions.append(EXPECTED_QUESTION_2)

    questionnaire = self.dao.get_with_children(1)
    
    self.assertEquals(sort_lists(expected_questionnaire.asdict(follow=CONCEPTS_AND_QUESTIONS)),
                      sort_lists(questionnaire.asdict(follow=CONCEPTS_AND_QUESTIONS)))
  
  def test_insert_duplicate(self):    
    q = Questionnaire(questionnaireId=1, resource='blah')
    self.dao.insert(q)
    try:
      self.dao.insert(q)
      self.fail("IntegrityError expected")
    except IntegrityError:
      pass    
  
  def test_update_no_expected_version(self):
    q = Questionnaire(resource='blah')
    q.concepts.append(CONCEPT_1)
    q.concepts.append(CONCEPT_2)
    q.questions.append(QUESTION_1)
    q.questions.append(QUESTION_2)
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(TIME):
      self.dao.insert(q)

    # Creating a questionnaire creates a history entry with children
    self.check_history()

    q = Questionnaire(questionnaireId=1, resource='foo')
    q.concepts.append(QuestionnaireConcept(conceptSystem='a', conceptCode='b'))
    q.concepts.append(QuestionnaireConcept(conceptSystem='x', conceptCode='y'))
    q.questions.append(QuestionnaireQuestion(linkId='x', conceptSystem='y', conceptCode='z'))
    q.questions.append(QuestionnaireQuestion(linkId='d', conceptSystem='e', conceptCode='f'))
    
    with FakeClock(TIME_2):
      self.dao.update(q)

    expected_questionnaire = Questionnaire(questionnaireId=1, version=2, created=TIME,
                                          lastModified=TIME_2, resource='foo')
    questionnaire = self.dao.get(1)
    self.assertEquals(expected_questionnaire.asdict(), questionnaire.asdict())

    # Updating a questionnaire keeps the existing history, and 
    # creates a new history element with children.
    # self.check_history()
    
    expected_history = QuestionnaireHistory(questionnaireId=1, version=2, created=TIME,
                                            lastModified=TIME_2, resource='foo')
    questionnaire_history = self.questionnaire_history_dao.get([1, 2])
    self.assertEquals(expected_history.asdict(), questionnaire_history.asdict())

    questionnaire_history = self.questionnaire_history_dao.get_with_children([1, 2])
    expected_concept_1 = QuestionnaireConcept(questionnaireConceptId=3, questionnaireId=1,
                                              questionnaireVersion=2, conceptSystem='a', 
                                              conceptCode='b')
    expected_concept_2 = QuestionnaireConcept(questionnaireConceptId=4, questionnaireId=1,
                                              questionnaireVersion=2, conceptSystem='x', 
                                              conceptCode='y')
    expected_question_1 = QuestionnaireQuestion(questionnaireQuestionId=3, questionnaireId=1,
                                                questionnaireVersion=2, linkId='x', 
                                                conceptSystem='y', conceptCode='z')
    expected_question_2 = QuestionnaireQuestion(questionnaireQuestionId=4, questionnaireId=1,
                                                questionnaireVersion=2, linkId='d', 
                                                conceptSystem='e', conceptCode='f')

    expected_history.concepts.append(expected_concept_1)
    expected_history.concepts.append(expected_concept_2)
    expected_history.questions.append(expected_question_1)
    expected_history.questions.append(expected_question_2)

    self.assertEquals(sort_lists(expected_history.asdict(follow=CONCEPTS_AND_QUESTIONS)),
                      sort_lists(questionnaire_history.asdict(follow=CONCEPTS_AND_QUESTIONS)))

    self.assertEquals(expected_concept_1.asdict(), self.questionnaire_concept_dao.get(3).asdict())
    self.assertEquals(expected_concept_2.asdict(), self.questionnaire_concept_dao.get(4).asdict())
    self.assertEquals(expected_question_1.asdict(), self.questionnaire_question_dao.get(3).asdict())
    self.assertEquals(expected_question_2.asdict(), self.questionnaire_question_dao.get(4).asdict())
  
  def test_update_right_expected_version(self):
    q = Questionnaire(resource='blah')
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(TIME):
      self.dao.insert(q)
    
    q = Questionnaire(questionnaireId=1, resource='foo')    
    with FakeClock(TIME_2):
      self.dao.update(q, expected_version=1)
    
    expected_questionnaire = Questionnaire(questionnaireId=1, version=2, created=TIME,
                                          lastModified=TIME_2, resource='foo')
    questionnaire = self.dao.get(1)
    self.assertEquals(expected_questionnaire.asdict(), questionnaire.asdict())\
  
  def test_update_wrong_expected_version(self):
    q = Questionnaire(resource='blah')
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(TIME):
      self.dao.insert(q)
    
    q = Questionnaire(questionnaireId=1, resource='foo')    
    with FakeClock(TIME_2):
      try:
        self.dao.update(q, expected_version=2)
        self.fail("PreconditionFailed expected")
      except PreconditionFailed:
        pass
      
  def test_update_not_exists(self):    
    q = Questionnaire(questionnaireId=1, resource='blah')
    try:
      self.dao.update(q)
      self.fail("NotFound expected")
    except NotFound:
      pass
  