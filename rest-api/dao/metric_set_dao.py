from dao.base_dao import UpsertableDao
from model.metric_set import AggregateMetrics, MetricSet
from dao import database_factory


class MetricSetDao(UpsertableDao):

  def __init__(self):
    super(MetricSetDao, self).__init__(MetricSet,
                                       db=database_factory.get_generic_database())

  @staticmethod
  def to_client_json(ms):
    return {
        'id': ms.metricSetId,
        'type': ms.metricSetType.name,
        'lastModifiedTime': ms.lastModified.isoformat()
    }


class AggregateMetricsDao(UpsertableDao):

  def __init__(self):
    super(AggregateMetricsDao, self).__init__(
        AggregateMetrics, db=database_factory.get_generic_database())

  def get_all_for_metric_set_with_session(self, session, ms_id):
    return (session.query(AggregateMetrics)
     .filter(AggregateMetrics.metricSetId == ms_id)
     .all())

  def get_all_for_metric_set(self, ms_id):
    with self.session() as session:
      return self.get_all_for_metric_set_with_session(session, ms_id)

  def delete_all_for_metric_set_with_session(self, session, ms_id):
    session.execute(AggregateMetrics.__table__.delete()
                    .where(AggregateMetrics.metricSetId == ms_id))

  def delete_all_for_metric_set(self, ms_id):
    with self.session() as session:
      self.delete_all_for_metric_set_with_session(session, ms_id)

  @staticmethod
  def to_client_json(aggs):
    by_key = {}
    for agg in aggs:
      if agg.metricsKey not in by_key:
        by_key[agg.metricsKey] = []
      by_key[agg.metricsKey].append(agg)

    out = []
    for (key, aggs) in by_key.iteritems():
      out.append({
          'key': key.name,
          'values': [{
              'value': agg.value,
              'count': agg.count
          } for agg in aggs]
      })
    return out
