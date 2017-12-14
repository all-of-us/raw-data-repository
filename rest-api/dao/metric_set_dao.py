from dao.base_dao import UpsertableDao
from model.metric_set import AggregateMetrics, MetricSet
from dao import database_factory


class MetricSetDao(UpsertableDao):

  def __init__(self):
    super(MetricSetDao, self).__init__(MetricSet,
                                       db=database_factory.get_generic_database())


class AggregateMetricsDao(UpsertableDao):

  def __init__(self):
    super(AggregateMetricsDao, self).__init__(
        AggregateMetrics, db=database_factory.get_generic_database())

  def delete_all_for_metric_set_with_session(self, session, ms_id):
    session.execute(AggregateMetrics.__table__.delete()
                    .where(AggregateMetrics.metricSetId == ms_id))

  def delete_all_for_metric_set(self, ms_id):
    with self.session() as session:
      self.delete_all_for_metric_set_with_session(session, ms_id)
