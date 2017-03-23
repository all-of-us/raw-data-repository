import datetime

from dao.code_dao import CodeDao
from dao.questionnaire_dao import QuestionnaireDao, QuestionnaireHistoryDao
from dao.questionnaire_dao import QuestionnaireConceptDao, QuestionnaireQuestionDao
from model.code import Code, CodeType
from model.questionnaire import Questionnaire, QuestionnaireHistory
from model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from unit_test_util import SqlTestBase, sort_lists
from clock import FakeClock
from werkzeug.exceptions import NotFound, PreconditionFailed
from sqlalchemy.exc import IntegrityError

EXPECTED_CONCEPT_1 = QuestionnaireConcept(questionnaireConceptId=1, questionnaireId=1,
                                          questionnaireVersion=1, codeId=1)
EXPECTED_CONCEPT_2 = QuestionnaireConcept(questionnaireConceptId=2, questionnaireId=1,
                                          questionnaireVersion=1, codeId=2)
EXPECTED_QUESTION_1 = QuestionnaireQuestion(questionnaireQuestionId=1, questionnaireId=1,
                                            questionnaireVersion=1, linkId='a', codeId=4,
                                            repeats=False)
EXPECTED_QUESTION_2 = QuestionnaireQuestion(questionnaireQuestionId=2, questionnaireId=1,
                                            questionnaireVersion=1, linkId='d', codeId=5,
                                            repeats=True)
TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
RESOURCE_1 = '{"x": "y"}'
RESOURCE_1_WITH_ID = '{"x": "y", "id": "1"}'
RESOURCE_2 = '{"x": "z"}'
RESOURCE_2_WITH_ID = '{"x": "z", "id": "1"}'

class QuestionnaireDaoTest(SqlTestBase):
  def setUp(self):
    super(QuestionnaireDaoTest, self).setUp(with_data=False)
    self.dao = QuestionnaireDao()
    self.questionnaire_history_dao = QuestionnaireHistoryDao()
    self.questionnaire_concept_dao = QuestionnaireConceptDao()
    self.questionnaire_question_dao = QuestionnaireQuestionDao()
    self.code_dao = CodeDao()
    self.CODE_1 = Code(codeId=1, system='a', value='b', display=u'c', topic=u'd',
                       codeType=CodeType.MODULE, mapped=True)
    self.CODE_2 = Code(codeId=2, system='a', value='x', display=u'y', codeType=CodeType.MODULE,
                       mapped=False)
    self.CODE_3 = Code(codeId=3, system='a', value='z', display=u'y', codeType=CodeType.MODULE,
                       mapped=False)
    self.CODE_4 = Code(codeId=4, system='a', value='c', codeType=CodeType.QUESTION, mapped=True,
                       parentId=1)
    self.CODE_5 = Code(codeId=5, system='a', value='d', codeType=CodeType.QUESTION, mapped=True,
                       parentId=2)
    self.CODE_6 = Code(codeId=6, system='a', value='e', codeType=CodeType.QUESTION, mapped=True,
                       parentId=2)
    self.CONCEPT_1 = QuestionnaireConcept(codeId=1)
    self.CONCEPT_2 = QuestionnaireConcept(codeId=2)
    self.QUESTION_1 = QuestionnaireQuestion(linkId='a', codeId=4, repeats=False)
    self.QUESTION_2 = QuestionnaireQuestion(linkId='d', codeId=5, repeats=True)
    self.insert_codes()

  def insert_codes(self):
    self.code_dao.insert(self.CODE_1)
    self.code_dao.insert(self.CODE_2)
    self.code_dao.insert(self.CODE_3)
    self.code_dao.insert(self.CODE_4)
    self.code_dao.insert(self.CODE_5)
    self.code_dao.insert(self.CODE_6)

  def test_get_before_insert(self):
    self.assertIsNone(self.dao.get(1))
    self.assertIsNone(self.dao.get_with_children(1))
    self.assertIsNone(self.dao.get_latest_questionnaire_with_concept(self.CODE_1.codeId))
    self.assertIsNone(self.questionnaire_history_dao.get([1, 1]))
    self.assertIsNone(self.questionnaire_history_dao.get_with_children([1, 1]))
    self.assertIsNone(self.questionnaire_concept_dao.get(1))
    self.assertIsNone(self.questionnaire_question_dao.get(1))

  def check_history(self):
    expected_history = QuestionnaireHistory(questionnaireId=1, version=1, created=TIME,
                                            lastModified=TIME, resource=RESOURCE_1_WITH_ID)
    questionnaire_history = self.questionnaire_history_dao.get([1, 1])
    self.assertEquals(expected_history.asdict(), questionnaire_history.asdict())

    questionnaire_history = self.questionnaire_history_dao.get_with_children([1, 1])
    expected_history.concepts.append(EXPECTED_CONCEPT_1)
    expected_history.concepts.append(EXPECTED_CONCEPT_2)
    expected_history.questions.append(EXPECTED_QUESTION_1)
    expected_history.questions.append(EXPECTED_QUESTION_2)

    self.assertEquals(EXPECTED_CONCEPT_1.asdict(), self.questionnaire_concept_dao.get(1).asdict())
    self.assertEquals(EXPECTED_CONCEPT_2.asdict(), self.questionnaire_concept_dao.get(2).asdict())
    self.assertEquals(EXPECTED_QUESTION_1.asdict(), self.questionnaire_question_dao.get(1).asdict())
    self.assertEquals(EXPECTED_QUESTION_2.asdict(), self.questionnaire_question_dao.get(2).asdict())

  def test_insert(self):
    q = Questionnaire(resource=RESOURCE_1)
    q.concepts.append(self.CONCEPT_1)
    q.concepts.append(self.CONCEPT_2)
    q.questions.append(self.QUESTION_1)
    q.questions.append(self.QUESTION_2)

    with FakeClock(TIME):
      self.dao.insert(q)

    # Creating a questionnaire creates a history entry with children
    self.check_history()

    expected_questionnaire = Questionnaire(questionnaireId=1, version=1, created=TIME,
                                          lastModified=TIME, resource=RESOURCE_1_WITH_ID)
    questionnaire = self.dao.get(1)
    self.assertEquals(expected_questionnaire.asdict(), questionnaire.asdict())

    expected_questionnaire.concepts.append(EXPECTED_CONCEPT_1)
    expected_questionnaire.concepts.append(EXPECTED_CONCEPT_2)
    expected_questionnaire.questions.append(EXPECTED_QUESTION_1)
    expected_questionnaire.questions.append(EXPECTED_QUESTION_2)

    questionnaire = self.dao.get_with_children(1)

    self.assertEquals(sort_lists(expected_questionnaire.asdict_with_children()),
                      sort_lists(questionnaire.asdict_with_children()))                      
    self.assertEquals(questionnaire.asdict(),
                      self.dao.get_latest_questionnaire_with_concept(self.CODE_1.codeId).asdict())

  def test_insert_duplicate(self):
    q = Questionnaire(questionnaireId=1, resource=RESOURCE_1)
    self.dao.insert(q)
    try:
      self.dao.insert(q)
      self.fail("IntegrityError expected")
    except IntegrityError:
      pass

  def test_update_right_expected_version(self):
    q = Questionnaire(resource=RESOURCE_1)
    with FakeClock(TIME):
      self.dao.insert(q)

    q = Questionnaire(questionnaireId=1, version=1, resource=RESOURCE_2)
    with FakeClock(TIME_2):
      self.dao.update(q)

    expected_questionnaire = Questionnaire(questionnaireId=1, version=2, created=TIME,
                                          lastModified=TIME_2, resource=RESOURCE_2)
    questionnaire = self.dao.get(1)
    self.assertEquals(expected_questionnaire.asdict(), questionnaire.asdict())

  def test_update_wrong_expected_version(self):
    q = Questionnaire(resource=RESOURCE_1)
    with FakeClock(TIME):
      self.dao.insert(q)

    q = Questionnaire(questionnaireId=1, version=2, resource=RESOURCE_2)
    with FakeClock(TIME_2):
      try:
        self.dao.update(q)
        self.fail("PreconditionFailed expected")
      except PreconditionFailed:
        pass

  def test_update_not_exists(self):
    q = Questionnaire(questionnaireId=1, resource=RESOURCE_1)
    try:
      self.dao.update(q)
      self.fail("NotFound expected")
    except NotFound:
      pass
      
  def test_insert_multiple_questionnaires_same_concept(self):
    q = Questionnaire(resource=RESOURCE_1)
    q.concepts.append(self.CONCEPT_1)
    q.concepts.append(self.CONCEPT_2)
    with FakeClock(TIME):
       self.dao.insert(q)
       
    q2 = Questionnaire(resource=RESOURCE_2)
    q2.concepts.append(self.CONCEPT_1)
    with FakeClock(TIME_2):
      self.dao.insert(q2)
      
    self.assertEquals(2, 
                      self.dao.get_latest_questionnaire_with_concept(self.CODE_1.codeId)
                        .questionnaireId)
    self.assertEquals(1, 
                      self.dao.get_latest_questionnaire_with_concept(self.CODE_2.codeId)
                        .questionnaireId)