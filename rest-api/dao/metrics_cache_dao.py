from model.metrics_cache import MetricsEnrollmentStatusCache, MetricsGenderCache, MetricsAgeCache
from dao.base_dao import BaseDao
from dao.hpo_dao import HPODao
from participant_enums import WithdrawalStatus
from participant_enums import TEST_HPO_NAME, TEST_EMAIL_PATTERN
import datetime

class MetricsEnrollmentStatusCacheDao(BaseDao):
  def __init__(self):
    super(MetricsEnrollmentStatusCacheDao, self).__init__(MetricsEnrollmentStatusCache)
    self.test_hpo_id = HPODao().get_by_name(TEST_HPO_NAME).hpoId
    self.test_email_pattern = TEST_EMAIL_PATTERN

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsEnrollmentStatusCache)
            .order_by(MetricsEnrollmentStatusCache.dateInserted.desc())
            .first())

  def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None):
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

      if hpo_ids:
        query = query.filter(MetricsEnrollmentStatusCache.hpoId.in_(hpo_ids))

      return query.all()

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

  def to_client_json(self, result_set):
    client_json = []
    for record in result_set:
      newItem = {
        'date': record.date.isoformat(),
        'hpo': record.hpoName,
        'metrics': {
          'registered': record.registeredCount,
          'consented': record.consentedCount,
          'core': record.coreCount,
        }
      }
      client_json.append(newItem)
    return client_json

  def get_total_interested_count(self, start_date, end_date, hpo_ids=None):
    if hpo_ids:
      filters_hpo = ' (' + ' OR '.join('p.hpo_id='+str(x) for x in hpo_ids) + ') AND '
    else:
      filters_hpo = ''

    sql = """
      SELECT
        SUM(CASE
         WHEN day>=sign_up_time THEN 1
         ELSE 0
        END) AS registered_count,
        day as start_date
        FROM (SELECT p.sign_up_time, calendar.day, p.hpo_id, hpo.name
             FROM participant p LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id,
                  calendar,
                  hpo
             WHERE %(filters_hpo)s p.hpo_id <> :test_hpo_id 
                AND p.hpo_id=hpo.hpo_id
                AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                AND p.withdrawal_status = :not_withdraw
                AND calendar.day >= :start_date
                AND calendar.day <= :end_date
            ) a
        GROUP BY day;
        """ % {'filters_hpo': filters_hpo}
    params = {'test_hpo_id': self.test_hpo_id,
              'not_withdraw': int(WithdrawalStatus.NOT_WITHDRAWN),
              'test_email_pattern': self.test_email_pattern, 'start_date': start_date,
              'end_date': end_date}

    results_by_date = []

    with self.session() as session:
      cursor = session.execute(sql, params)
    try:
      results = cursor.fetchall()
      for result in results:
        date = result[1]
        metrics = {'TOTAL': int(result[0])}
        results_by_date.append({
          'date': str(date),
          'metrics': metrics
        })

    finally:
      cursor.close()

    return results_by_date

  def get_metrics_cache_sql(self):
    sql = """
      insert into metrics_enrollment_status_cache
        SELECT
        :date_inserted as date_inserted,
        hpo_id,
        name,
        day as date,
        sum(CASE
          WHEN day>=sign_up_time AND (enrollment_status_member_time IS NULL OR day < enrollment_status_member_time) THEN 1
          ELSE 0
        END) AS registered_count,
        sum(CASE
          WHEN enrollment_status_member_time IS NOT NULL AND day>=enrollment_status_member_time AND (enrollment_status_core_stored_sample_time IS NULL OR day < enrollment_status_core_stored_sample_time) THEN 1
          ELSE 0
        END) AS consented_count,
        sum(CASE
          WHEN enrollment_status_core_stored_sample_time IS NOT NULL AND day>=enrollment_status_core_stored_sample_time THEN 1
          ELSE 0
        END) AS core_count
        FROM (SELECT p.sign_up_time, ps.enrollment_status_member_time, ps.enrollment_status_core_ordered_sample_time, ps.enrollment_status_core_stored_sample_time, calendar.day, p.hpo_id, hpo.name
              FROM participant p LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id,
                   calendar,
                   hpo
              WHERE p.hpo_id=:hpo_id and p.hpo_id <> :test_hpo_id
                AND p.hpo_id=hpo.hpo_id
                AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                AND p.withdrawal_status = :not_withdraw
                AND calendar.day >= :start_date
                AND calendar.day <= :end_date
             ) a
        GROUP BY day, hpo_id, name;
    """
    return sql

class MetricsGenderCacheDao(BaseDao):
  def __init__(self):
    super(MetricsGenderCacheDao, self).__init__(MetricsGenderCache)

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsGenderCache)
            .order_by(MetricsGenderCache.dateInserted.desc())
            .first())

  def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None):
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

      if hpo_ids:
        query = query.filter(MetricsGenderCache.hpoId.in_(hpo_ids))

      return query.all()

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
            'UNSET': 0,
            'Woman': 0,
            'Man': 0,
            'Transgender': 0,
            'PMI_Skip': 0,
            'Non-Binary': 0,
            'Other/Additional Options': 0,
            'UNMAPPED': 0
          }
        }
        new_item['metrics'][record.genderName] = record.genderCount
        client_json.append(new_item)
    return client_json

  def get_metrics_cache_sql(self):
    sql = """
          insert into metrics_gender_cache
            SELECT
            :date_inserted as date_inserted,
            hpo_id,
            name as hpo_name,
            day as date,
            gender_name,
            COUNT(*) AS gender_count
            from
            (
                SELECT p.participant_id, p.hpo_id, hpo.name, p.sign_up_time,day,
                   CASE
                       WHEN ps.gender_identity_id IS NULL OR ps.gender_identity_id=924 THEN 'UNSET'
                       WHEN ps.gender_identity_id=354 THEN 'Woman'
                       WHEN ps.gender_identity_id=356 THEN 'Man'
                       WHEN ps.gender_identity_id=355 THEN 'Transgender'
                       WHEN ps.gender_identity_id=930 THEN 'PMI_Skip'
                       WHEN ps.gender_identity_id=358 THEN 'Non-Binary'
                       WHEN ps.gender_identity_id=357 THEN 'Other/Additional Options'
                     ELSE 'UNSET'
                   END gender_name
                FROM participant p LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id,
                     calendar,
                     hpo
                WHERE p.hpo_id=:hpo_id and p.hpo_id <> :test_hpo_id
                AND p.hpo_id = hpo.hpo_id
                AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                AND p.withdrawal_status = :not_withdraw
                AND calendar.day >= :start_date
                AND calendar.day <= :end_date
                AND calendar.day >= p.sign_up_time
            ) x
            GROUP BY day, hpo_id, hpo_name, gender_name
            ;
        """
    return sql

class MetricsAgeCacheDao(BaseDao):
  def __init__(self):
    super(MetricsAgeCacheDao, self).__init__(MetricsAgeCache)

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsAgeCache)
            .order_by(MetricsAgeCache.dateInserted.desc())
            .first())

  def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is None:
        return None
      last_inserted_date = last_inserted_record.dateInserted
      query = session.query(MetricsAgeCache)\
        .filter(MetricsAgeCache.dateInserted == last_inserted_date)
      if start_date:
        query = query.filter(MetricsAgeCache.date >= start_date)
      if end_date:
        query = query.filter(MetricsAgeCache.date <= end_date)

      if hpo_ids:
        query = query.filter(MetricsAgeCache.hpoId.in_(hpo_ids))

      return query.all()

  def delete_old_records(self, n_days_ago=7):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is not None:
        last_date_inserted = last_inserted_record.dateInserted
        seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
        delete_sql = """
          delete from metrics_age_cache where date_inserted < :seven_days_ago
        """
        params = {'seven_days_ago': seven_days_ago}
        session.execute(delete_sql, params)

  def to_client_json(self, result_set):
    client_json = []
    for record in result_set:
      is_exist = False
      for item in client_json:
        if item['date'] == record.date.isoformat() and item['hpo'] == record.hpoName:
          item['metrics'][record.ageRange] = record.ageCount
          is_exist = True
      if not is_exist:
        new_item = {
          'date': record.date.isoformat(),
          'hpo': record.hpoName,
          'metrics': {
            'UNSET': 0,
            '0-17': 0,
            '18-25': 0,
            '26-35': 0,
            '36-45': 0,
            '46-55': 0,
            '56-65': 0,
            '66-75': 0,
            '76-85': 0,
            '86-': 0
          }
        }
        new_item['metrics'][record.ageRange] = record.ageCount
        client_json.append(new_item)
    return client_json

  def get_metrics_cache_sql(self):
    sql = """
          insert into metrics_age_cache
            SELECT
            :date_inserted as date_inserted,
            hpo_id,
            name as hpo_name,
            day as date,
            age_range,
            COUNT(*) AS age_count
            from
            (
                SELECT hpo_id, name, sign_up_time, day,
                   CASE
                       WHEN age IS NULL THEN 'UNSET'
                       WHEN age>=0 and age<=17 THEN '0-17'
                       WHEN age>=18 and age<=25 THEN '18-25'
                       WHEN age>=26 and age<=35 THEN '26-35'
                       WHEN age>=36 and age<=45 THEN '36-45'
                       WHEN age>=46 and age<=55 THEN '46-55'
                       WHEN age>=56 and age<=65 THEN '56-65'
                       WHEN age>=66 and age<=75 THEN '66-75'
                       WHEN age>=76 and age<=85 THEN '76-85'
                       WHEN age>=86 THEN '86-'
                   end age_range
                from
                (
                    SELECT p.participant_id, p.hpo_id, hpo.name, p.sign_up_time,day,ps.date_of_birth,
                       CASE
                         WHEN ps.date_of_birth IS NOT NULL THEN Date_format(From_Days( To_Days(day) - To_Days(ps.date_of_birth) ), '%Y' ) + 0
                         ELSE NULL
                       END age
                    FROM participant p LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id,
                         calendar,
                         hpo
                    WHERE p.hpo_id=:hpo_id and p.hpo_id <> :test_hpo_id
                    AND p.hpo_id = hpo.hpo_id
                    AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                    AND p.withdrawal_status = :not_withdraw
                    AND calendar.day >= :start_date
                    AND calendar.day <= :end_date
                    AND calendar.day >= p.sign_up_time
                ) x

            ) y
            GROUP BY day, hpo_id, hpo_name, age_range
            ;
        """
    return sql
