import clock
import json
import logging

from model.metrics import MetricsVersion, MetricsBucket
from dao.base_dao import BaseDao, UpsertableDao
from werkzeug.exceptions import PreconditionFailed
from sqlalchemy.orm import subqueryload
from datetime import timedelta

# Only metrics that match this version will be served.  See the corresponding
# comment in metrics_pipeline.py.
SERVING_METRICS_DATA_VERSION = 1

METRICS_LOCK_TIMEOUT = timedelta(hours=24)
# Delete old metrics after 3 days. (They generally won't be used after one successful pipeline run,
# but we'll keep them around in case we need to poke at them for a few days.)
_METRICS_EXPIRATION = timedelta(days=3)

class MetricsVersionDao(BaseDao):
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
          session.merge(running_version)
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
        session.merge(running_version)
      else:
        logging.warn('Metrics pipeline is not running; not setting as finished')

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsVersion)
        .filter(MetricsVersion.complete == True)
        .filter(MetricsVersion.dataVersion == SERVING_METRICS_DATA_VERSION)
        .order_by(MetricsVersion.date.desc())
        .first())

  def get_serving_version(self):
    with self.session() as session:
      return self.get_serving_version_with_session(session)

  def delete_old_versions(self):
    with self.session() as session:
      old_date = clock.CLOCK.now() - _METRICS_EXPIRATION
      old_versions = (session.query(MetricsVersion)
        .filter(MetricsVersion.date < old_date)
        .all())
      for version in old_versions:
        session.delete(version)

class MetricsBucketDao(UpsertableDao):

  def __init__(self):
    super(MetricsBucketDao, self).__init__(MetricsBucket)

  def get_id(self, obj):
    return [obj.metricsVersionId, obj.date, obj.hpoId]

  def get_active_buckets(self, start_date=None, end_date=None):
    with self.session() as session:
      version = MetricsVersionDao().get_serving_version_with_session(session)
      if version is None:
        return None
      version_id = version.metricsVersionId
      query = session.query(MetricsBucket).filter(MetricsBucket.metricsVersionId == version_id)
      if start_date:
        query = query.filter(MetricsBucket.date >= start_date)
      if end_date:
        query = query.filter(MetricsBucket.date <= end_date)
      return query.order_by(MetricsBucket.date).order_by(MetricsBucket.hpoId).all()

  def to_client_json(self, model):
    facets = {'date': model.date.isoformat()}
    if model.hpoId:
      facets['hpoId'] = model.hpoId
    return {'facets': facets, 'entries': json.loads(model.metrics)}
