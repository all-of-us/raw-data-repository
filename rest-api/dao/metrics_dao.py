import clock
import logging

from model.metrics import MetricsVersion, MetricsBucket
from dao.base_dao import BaseDao, UpdatableDao
from werkzeug.exceptions import BadRequest, PreconditionFailed, NotFound
from sqlalchemy.orm import subqueryload
from datetime import timedelta

# Only metrics that match this version will be served.  See the corresponding
# comment in metrics_pipeline.py.
SERVING_METRICS_DATA_VERSION = 1

METRICS_LOCK_TIMEOUT = timedelta(hours=24)

class MetricsVersionDao(UpdatableDao):  
  def __init__(self):
    super(MetricsVersionDao, self).__init__(MetricsVersion)

  def get_id(self, obj):
    return obj.metricsVersionId

  def get_with_children(self, metrics_version_id):
    with self.session() as session:
      query = session.query(MetricsVersion).options(subqueryload(MetricsVersion.buckets))
      return query.get(metrics_version_id)
  
  def get_version_in_progress_with_session(self, session):
    return (session.query(MetricsVersion)
        .filter(MetricsVersion.inProgress == True)
        .order_by(MetricsVersion.date.desc())
        .first())

  def get_version_in_progress(self):
    with self.session() as session:
      return self.get_version_in_progress_with_session(session)
  
  def set_pipeline_in_progress(self):
    with self.session() as session:
      running_version = self.get_version_in_progress_with_session(session)
      if running_version:
        if running_version.date + METRICS_LOCK_TIMEOUT <= clock.CLOCK.now():
          logging.warning("Metrics version %s timed out; breaking lock." % 
                          running_version.metricsVersionId)
          running_version.inProgress = False
          self.update_with_session(session, running_version)
        else: 
          # If the timeout hasn't elapsed, don't allow a new pipeline to start.
          raise PreconditionFailed('Metrics pipeline is already running.')
      new_version = MetricsVersion(inProgress=True, dataVersion=SERVING_METRICS_DATA_VERSION)
      self.insert_with_session(session, new_version)
      return new_version.metricsVersionId      
 
  def set_pipeline_finished(self, complete):
    with self.session() as session:
      running_version = self.get_version_in_progress_with_session(session)
      if running_version:
        running_version.inProgress = False
        running_version.complete = complete
        self.update_with_session(session, running_version)
      else:
        raise PreconditionFailed('Metrics pipeline is not running')

  def get_serving_version(self):
    with self.session() as session:
      return (session.query(MetricsVersion)
        .filter(MetricsVersion.complete == True)
        .filter(MetricsVersion.dataVersion == SERVING_METRICS_DATA_VERSION)
        .options(subqueryload(MetricsVersion.buckets))
        .order_by(MetricsVersion.date.desc())
        .first())
        
class MetricsBucketDao(BaseDao):

  def __init__(self):
    super(MetricsBucketDao, self).__init__(MetricsBucket)

  def get_id(self, obj):
    return [obj.metricsVersionId, obj.date, obj.hpoId]
