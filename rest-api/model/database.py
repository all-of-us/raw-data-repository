from contextlib import contextmanager

from model.base import Base
# All tables in the schema should be imported below here.
# pylint: disable=unused-import
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_stored_sample import BiobankStoredSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.code import CodeBook, Code, CodeHistory
from model.hpo import HPO
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements
from model.metrics import MetricsVersion, MetricsBucket
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from model.questionnaire import QuestionnaireConcept
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from model.site import Site

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Database(object):
  """Maintains state for accessing the database."""
  def __init__(self, database_uri, **kwargs):
    # Add echo=True here to spit out SQL statements.
    self._engine = create_engine(database_uri, **kwargs)
    self.db_type = database_uri.split(':')[0]
    if self.db_type == 'sqlite':
      self._engine.execute('PRAGMA foreign_keys = ON;')
    # expire_on_commit = False allows us to access model objects outside of a transaction.
    # It also means that after a commit, a model object won't read from the database for its
    # properties. (Which should be fine.)
    self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)

  def get_engine(self):
    return self._engine

  def create_schema(self):
    Base.metadata.create_all(self._engine)

  def make_session(self):
    return self._Session()

  @contextmanager
  def session(self):
    sess = self.make_session()
    try:
      yield sess
      sess.commit()
    except Exception:
      sess.rollback()
      raise
    finally:
      sess.close()
