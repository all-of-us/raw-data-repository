import os

from model.base import Base
# All tables in the schema should be imported below here.
from model.config import Config
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_sample import BiobankSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderSample
from model.hpo import HPO
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements
from model.metrics import MetricsVersion, MetricsBucket
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from model.questionnaire import QuestionnaireConcept
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing.plugin.plugin_base import _engine_uri

class Database(object):
  """Maintains state for accessing the database."""
  def __init__(self, database_uri, **kwargs):
    self._engine = create_engine(database_uri, **kwargs)
    self._engine.execute('PRAGMA foreign_keys = ON;')
    self.Session = sessionmaker(bind=self._engine)

  def get_engine(self):
    return self._engine
    
  def create_schema(self):
    Base.metadata.create_all(self._engine)
    
  def make_session(self):    
    return self.Session()
    

  
  