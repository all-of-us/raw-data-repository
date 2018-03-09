from contextlib import contextmanager
import logging

from model.base import Base, MetricsBase
# All tables in the schema should be imported below here.
# pylint: disable=unused-import
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_stored_sample import BiobankStoredSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.code import CodeBook, Code, CodeHistory
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

from sqlalchemy import create_engine, event, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker


class Database(object):
  """Maintains state for accessing the database."""
  def __init__(self, url, **kwargs):
    # Add echo=True here to spit out SQL statements.
    # Set pool_recycle to 3600 -- one hour in seconds -- which is lower than the MySQL wait_timeout
    # parameter (which defaults to 8 hours) to ensure that we don't attempt to use idle database
    # connections after this period. (See DA-237.) To change the db wait_timeout (seconds), run:
    # gcloud --project <proj> sql instances patch rdrmaindb --database-flags wait_timeout=28800
    self._engine = create_engine(url, pool_recycle=3600, echo=True, **kwargs)
    event.listen(self._engine, 'engine_connect', _ping_connection)
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


def _ping_connection(connection, branch):
  """Makes sure connections are alive before trying to use them.

  Copied from SQLAlchemy 1.1 docs:
  http://docs.sqlalchemy.org/en/rel_1_1/core/pooling.html#disconnect-handling-pessimistic
  TODO(DA-321) Once SQLAlchemy v1.2 is out of development and released, switch to
  create_engine(pool_pre_ping=True).
  """
  if branch:
    # "branch" refers to a sub-connection of a connection,
    # we don't want to bother pinging on these.
    return

  # turn off "close with result".  This flag is only used with
  # "connectionless" execution, otherwise will be False in any case
  save_should_close_with_result = connection.should_close_with_result
  connection.should_close_with_result = False

  try:
    # run a SELECT 1.   use a core select() so that
    # the SELECT of a scalar value without a table is
    # appropriately formatted for the backend
    connection.scalar(select([1]))
  except DBAPIError as err:
    # catch SQLAlchemy's DBAPIError, which is a wrapper
    # for the DBAPI's exception.  It includes a .connection_invalidated
    # attribute which specifies if this connection is a "disconnect"
    # condition, which is based on inspection of the original exception
    # by the dialect in use.
    logging.warning('Database connection ping failed.', exc_info=True)
    if err.connection_invalidated:
      # run the same SELECT again - the connection will re-validate
      # itself and establish a new connection.  The disconnect detection
      # here also causes the whole connection pool to be invalidated
      # so that all stale connections are discarded.
      logging.warning('Database connection invalidated, reconnecting.')
      connection.scalar(select([1]))
    else:
      raise
  finally:
    # restore "close with result"
    connection.should_close_with_result = save_should_close_with_result
