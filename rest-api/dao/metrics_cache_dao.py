from model.metrics_cache import MetricsEnrollmentStatusCache, MetricsGenderCache
from dao.base_dao import BaseDao
import datetime

class MetricsEnrollmentStatusCacheDao(BaseDao):
  def __init__(self):
    super(MetricsEnrollmentStatusCacheDao, self).__init__(MetricsEnrollmentStatusCache)

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsEnrollmentStatusCache)
            .order_by(MetricsEnrollmentStatusCache.dateInserted.desc())
            .first())

  def get_active_buckets(self, start_date=None, end_date=None):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is None:
        return None
      last_inserted_date = last_inserted_record.dateInserted
      query = session.query(MetricsEnrollmentStatusCache)\
        .filter(MetricsEnrollmentStatusCache.dateInserted == last_inserted_date)
      if start_date:
        query = query.filter(MetricsEnrollmentStatusCache.date >= start_date)
      if end_date:
        query = query.filter(MetricsEnrollmentStatusCache.date <= end_date)
      return query.order_by(MetricsEnrollmentStatusCache.date)\
        .order_by(MetricsEnrollmentStatusCache.hpoId).all()

  def delete_old_records(self, n_days_ago=7):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is not None:
        last_date_inserted = last_inserted_record.dateInserted
        seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
        delete_sql = """
          delete from metrics_enrollment_status_cache where date_inserted < :seven_days_ago
        """
        params = {'seven_days_ago': seven_days_ago}
        session.execute(delete_sql, params)

  def to_client_json(self, model):
    return {
      'date': model.date.isoformat(),
      'hpo': model.hpoName,
      'metrics': {
        'registered': model.registeredCount,
        'consented': model.consentedCount,
        'core': model.coreCount,
      }
    }

class MetricsGenderCacheDao(BaseDao):
  def __init__(self):
    super(MetricsGenderCacheDao, self).__init__(MetricsGenderCache)

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsGenderCache)
            .order_by(MetricsGenderCache.dateInserted.desc())
            .first())

  def get_active_buckets(self, start_date=None, end_date=None):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is None:
        return None
      last_inserted_date = last_inserted_record.dateInserted
      query = session.query(MetricsGenderCache)\
        .filter(MetricsGenderCache.dateInserted == last_inserted_date)
      if start_date:
        query = query.filter(MetricsGenderCache.date >= start_date)
      if end_date:
        query = query.filter(MetricsGenderCache.date <= end_date)
      return query.order_by(MetricsGenderCache.date)\
        .order_by(MetricsGenderCache.hpoId).all()

  def delete_old_records(self, n_days_ago=7):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is not None:
        last_date_inserted = last_inserted_record.dateInserted
        seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
        delete_sql = """
          delete from metrics_gender_cache where date_inserted < :seven_days_ago
        """
        params = {'seven_days_ago': seven_days_ago}
        session.execute(delete_sql, params)

  def to_client_json(self, result_set):
    client_json = []
    for record in result_set:
      is_exist = False
      for item in client_json:
        if item['date'] == record.date.isoformat() and item['hpo'] == record.hpoName:
          item['metrics'][record.genderName] = record.genderCount
          is_exist = True
      if not is_exist:
        new_item = {
          'date': record.date.isoformat(),
          'hpo': record.hpoName,
          'metrics': {
            record.genderName: record.genderCount
          }
        }
        client_json.append(new_item)
    return client_json

