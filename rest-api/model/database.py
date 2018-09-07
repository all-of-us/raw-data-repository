from contextlib import contextmanager

import backoff
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from model.base import Base, MetricsBase
# All tables in the schema should be imported below here.
# pylint: disable=unused-import
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_stored_sample import BiobankStoredSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.code import CodeBook, Code, CodeHistory
from model.calendar import Calendar
from model.hpo import HPO
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements, Measurement
from model.metric_set import AggregateMetrics, MetricSet
from model.metrics import MetricsVersion, MetricsBucket
from model.organization import Organization
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from model.questionnaire import QuestionnaireConcept
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from model.site import Site

RETRY_CONNECTION_LIMIT = 10


class Database(object):
  """Maintains state for accessing the database."""

  def __init__(self, url, **kwargs):
    # Add echo=True here to spit out SQL statements.
    # Set pool_recycle to 3600 -- one hour in seconds -- which is lower than the MySQL wait_timeout
    # parameter (which defaults to 8 hours) to ensure that we don't attempt to use idle database
    # connections after this period. (See DA-237.) To change the db wait_timeout (seconds), run:
    # gcloud --project <proj> sql instances patch rdrmaindb --database-flags wait_timeout=28800
    self._engine = create_engine(url, echo=True, pool_pre_ping=True, pool_recycle=3600, **kwargs)
    self.db_type = url.drivername
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

  def create_metrics_schema(self):
    MetricsBase.metadata.create_all(self._engine)

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

  # TODO: at the time of writing, a PR went in to backoff that adds a `max_time` parameter.  This
  # will be part of backoff 1.5 (we are on backoff 1.4).  When it releases, use it to prevent
  # ludicriously long retry periods.
  @backoff.on_exception(backoff.expo,
                        DBAPIError,
                        max_tries=RETRY_CONNECTION_LIMIT,
                        giveup=lambda err: not getattr(err, 'connection_invalidated', False))
  def autoretry(self, func):
    """Runs a function of the db session and attempts to commit.  If we encounter a dropped
    connection, we retry the operation.  The retries are spaced out using exponential backoff with
    full jitter.  All other errors are propagated.
    """
    with self.session() as session:
      return func(session)
