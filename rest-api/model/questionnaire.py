import fhirclient.models.questionnaire

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, DateTime, BLOB, String, ForeignKeyConstraint, Index
from sqlalchemy import UniqueConstraint
from werkzeug.exceptions import BadRequest

CONCEPTS_AND_QUESTIONS = {'concepts':{}, 'questions':{}}

class QuestionnaireBase(object):
  """Mixin containing columns for Questionnaire and QuestionnaireHistory"""
  questionnaireId = Column('questionnaire_id', Integer, primary_key=True)
  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)      
  created = Column('created', DateTime, nullable=False)
  lastModified = Column('last_modified', DateTime, nullable=False)
  resource = Column('resource', BLOB, nullable=False)  

  def asdict_with_children(self):
    return self.asdict(follow=CONCEPTS_AND_QUESTIONS)
  
  def to_json(self):
    return self.resource    
        
class Questionnaire(QuestionnaireBase, Base):  
  """A questionnaire containing questions to pose to participants."""
  __tablename__ = 'questionnaire'
  concepts = relationship('QuestionnaireConcept', cascade="expunge", cascade_backrefs=False,
                          primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireConcept.questionnaireId)')
  questions = relationship('QuestionnaireQuestion', cascade="expunge", cascade_backrefs=False,
                           primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireQuestion.questionnaireId)')
                            
  @classmethod
  def from_json(cls, json, id=None, expected_version=None):
    fhir_q = fhirclient.models.questionnaire.Questionnaire(json)
    if not fhir_q.group:
      raise BadRequest("No top-level group found in questionnaire")
    
    q = Questionnaire(resource=json, questionnaireId=id, version=expected_version)
    if fhir_q.group.concept:
      for concept in fhir_q.group.concept:
        if concept.system and concept.code:
          q.concepts.append(QuestionnaireConcept(conceptSystem=concept.system, 
                                                 conceptCode=concept.code))    
    Questionnaire._populate_questions(fhir_q.group, q)
    return q
  
  @classmethod
  def _populate_questions(cls, group, q):
    """Recursively populate questions under this group."""
    if group.question:
      for question in group.question:
        # Capture any questions that have a link ID and single concept with a system and code
        if question.linkId and question.concept and len(question.concept) == 1 :
          concept = question.concept[0]
          if concept.system and concept.code:
            q.questions.append(QuestionnaireQuestion(linkId=question.linkId,
                                                     conceptSystem=concept.system,
                                                     conceptCode=concept.code))
        if question.group:
          for sub_group in question.group:
            Questionnaire._populate_questions(sub_group, q)    
    if group.group:
      for sub_group in group.group:
        Questionnaire._populate_questions(sub_group, q)
      
class QuestionnaireHistory(QuestionnaireBase, Base):  
  __tablename__ = 'questionnaire_history'
  version = Column('version', Integer, primary_key=True)
  concepts = relationship('QuestionnaireConcept', cascade='all, delete-orphan')
  questions = relationship('QuestionnaireQuestion', cascade='all, delete-orphan')  

class QuestionnaireConcept(Base):
  """Concepts for the questionnaire as a whole. These should be copied whenever a new version of 
  a questionnaire is created."""
  __tablename__ = 'questionnaire_concept'
  questionnaireConceptId = Column('questionnaire_concept_id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer, nullable=False)
  questionnaireVersion = Column('questionnaire_version', Integer, nullable=False)
  conceptSystem = Column('concept_system', String(50), nullable=False)
  conceptCode = Column('concept_code', String(20), nullable=False)
  __table_args__ = (
    ForeignKeyConstraint(
        ['questionnaire_id', 'questionnaire_version'], 
        ['questionnaire_history.questionnaire_id', 'questionnaire_history.version']),
    UniqueConstraint('questionnaire_id', 'questionnaire_version', 'concept_system', 'concept_code')
  )
Index('questionnaire_concept_system_code', QuestionnaireConcept.conceptSystem, 
      QuestionnaireConcept.conceptCode)

class QuestionnaireQuestion(Base):
  """A question in a questionnaire. These should be copied whenever a new version of a 
  questionnaire is created.

  Each question has a concept system and code defining what the question is about. Questions on
  different questionnaires can share the same concept code, but concept code is unique within a
  given questionnaire.
  """
  __tablename__ = 'questionnaire_question'
  questionnaireQuestionId = Column('questionnaire_question_id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer)
  questionnaireVersion = Column('questionnaire_version', Integer)
  linkId = Column('link_id', String(20))
  conceptSystem = Column('concept_system', String(50))
  conceptCode = Column('concept_code', String(20))
  # Should we also include valid answers here?  
  __table_args__ = (
    ForeignKeyConstraint(
        ['questionnaire_id', 'questionnaire_version'],
        ['questionnaire_history.questionnaire_id', 'questionnaire_history.version']),
    UniqueConstraint('questionnaire_id', 'questionnaire_version', 'link_id')
  )
  
Index('questionnaire_question_system_code', QuestionnaireQuestion.conceptSystem, 
      QuestionnaireQuestion.conceptCode)
