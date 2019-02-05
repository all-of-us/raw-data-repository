from model.metrics_cache import MetricsEnrollmentStatusCache, MetricsGenderCache, MetricsAgeCache, \
  MetricsRaceCache
from dao.base_dao import BaseDao
from dao.hpo_dao import HPODao
from dao.code_dao import CodeDao
from participant_enums import TEST_HPO_NAME, TEST_EMAIL_PATTERN
from code_constants import PPI_SYSTEM
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
      new_item = {
        'date': record.date.isoformat(),
        'hpo': record.hpoName,
        'metrics': {
          'registered': record.registeredCount,
          'consented': record.consentedCount,
          'core': record.coreCount,
        }
      }
      client_json.append(new_item)
    return client_json

  def get_total_interested_count(self, start_date, end_date, hpo_ids=None):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is None:
        return []
      last_inserted_date = last_inserted_record.dateInserted

      if hpo_ids:
        filters_hpo = ' (' + ' OR '.join('hpo_id='+str(x) for x in hpo_ids) + ') AND '
      else:
        filters_hpo = ''
      sql = """
        SELECT (SUM(registered_count) + SUM(consented_count) + SUM(core_count)) AS registered_count,
        date AS start_date
        FROM metrics_enrollment_status_cache
        WHERE %(filters_hpo)s
        date_inserted=:date_inserted
        AND date >= :start_date
        AND date <= :end_date
        GROUP BY date;
      """ % {'filters_hpo': filters_hpo}
      params = {'start_date': start_date, 'end_date': end_date, 'date_inserted': last_inserted_date}

      results_by_date = []

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
                :date_inserted AS date_inserted,
                :hpo_id AS hpo_id,
                (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS name,
                c.day AS date,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(p.sign_up_time) AS sign_up_time,
                           DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                           DATE(ps.enrollment_status_core_stored_sample_time) AS enrollment_status_core_stored_sample_time,
                           count(*) enrollment_count
                    FROM participant p
                           LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
                    WHERE p.hpo_id = :hpo_id AND p.hpo_id <> :test_hpo_id
                      AND p.is_ghost_id IS NOT TRUE
                      AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                      AND p.withdrawal_status = :not_withdraw
                    GROUP BY DATE(p.sign_up_time), DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time)
                  ) AS results
                  WHERE c.day>=DATE(sign_up_time) AND (enrollment_status_member_time IS NULL OR c.day < DATE(enrollment_status_member_time))
                ),0) AS registered_count,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(p.sign_up_time) AS sign_up_time,
                           DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                           DATE(ps.enrollment_status_core_stored_sample_time) AS enrollment_status_core_stored_sample_time,
                           count(*) enrollment_count
                    FROM participant p
                           LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
                    WHERE p.hpo_id = :hpo_id AND p.hpo_id <> :test_hpo_id
                      AND p.is_ghost_id IS NOT TRUE
                      AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                      AND p.withdrawal_status = :not_withdraw
                    GROUP BY DATE(p.sign_up_time), DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time)
                  ) AS results
                  WHERE enrollment_status_member_time IS NOT NULL AND day>=DATE(enrollment_status_member_time) AND (enrollment_status_core_stored_sample_time IS NULL OR day < DATE(enrollment_status_core_stored_sample_time))
                ),0) AS consented_count,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(p.sign_up_time) AS sign_up_time,
                           DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                           DATE(ps.enrollment_status_core_stored_sample_time) AS enrollment_status_core_stored_sample_time,
                           count(*) enrollment_count
                    FROM participant p
                           LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
                    WHERE p.hpo_id = :hpo_id AND p.hpo_id <> :test_hpo_id
                      AND p.is_ghost_id IS NOT TRUE
                      AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                      AND p.withdrawal_status = :not_withdraw
                    GROUP BY DATE(p.sign_up_time), DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time)
                  ) AS results
                  WHERE enrollment_status_core_stored_sample_time IS NOT NULL AND day>=DATE(enrollment_status_core_stored_sample_time)
                ),0) AS core_count
              FROM calendar c
              WHERE c.day BETWEEN :start_date AND :end_date
              ;
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
            'Prefer not to say': 0,
            'UNMAPPED': 0
          }
        }
        new_item['metrics'][record.genderName] = record.genderCount
        client_json.append(new_item)
    return client_json

  def get_metrics_cache_sql(self):
    sql = """insert into metrics_gender_cache """
    gender_names = ['UNSET', 'Woman', 'Man', 'Transgender', 'PMI_Skip', 'Non-Binary',
                    'Other/Additional Options', 'Prefer not to say']
    gender_conditions = [
      ' ps.gender_identity_id IS NULL ',
      ' ps.gender_identity_id=354 ',
      ' ps.gender_identity_id=356 ',
      ' ps.gender_identity_id=355 ',
      ' ps.gender_identity_id=930 ',
      ' ps.gender_identity_id=358 ',
      ' ps.gender_identity_id=357 ',
      ' ps.gender_identity_id=924 '
    ]
    sub_queries = []
    sql_template = """
      SELECT
        :date_inserted AS date_inserted,
        :hpo_id AS hpo_id,
        (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
        c.day AS date,
        '{0}' AS gender_name,  
        IFNULL((
          SELECT SUM(results.gender_count)
          FROM
          (
            SELECT DATE(p.sign_up_time) as day,
                   COUNT(*) gender_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
                   LEFT JOIN hpo ON p.hpo_id=hpo.hpo_id
            WHERE p.hpo_id = :hpo_id AND p.hpo_id <> :test_hpo_id
              AND p.is_ghost_id IS NOT TRUE
              AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
              AND p.withdrawal_status = :not_withdraw
              AND {1}
            GROUP BY DATE(p.sign_up_time)
          ) AS results
          WHERE results.day <= c.day
        ),0) AS gender_count
      FROM calendar c
      WHERE c.day BETWEEN :start_date AND :end_date
    """
    for gender_name, gender_condition in zip(gender_names, gender_conditions):
      sub_query = sql_template.format(gender_name, gender_condition)
      sub_queries.append(sub_query)

    sql += ' union '.join(sub_queries)

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
          :date_inserted AS date_inserted,
          :hpo_id AS hpo_id,
          (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
          c.day AS date,
          'UNSET' AS age_range,
          IFNULL((
            SELECT SUM(results.age_count)
            FROM
            (
              SELECT DATE(p.sign_up_time) AS day,
                     count(*) age_count
              FROM participant p
                     LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
                     LEFT JOIN hpo ON p.hpo_id=hpo.hpo_id
              WHERE p.hpo_id = :hpo_id AND p.hpo_id <> :test_hpo_id
                AND p.is_ghost_id IS NOT TRUE
                AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                AND p.withdrawal_status = :not_withdraw
                AND ps.date_of_birth IS NULL
              GROUP BY DATE(p.sign_up_time)
            ) AS results
            WHERE results.day <= c.day
          ),0) AS age_count
        FROM calendar c
        WHERE c.day BETWEEN :start_date AND :end_date
        UNION
    """
    age_ranges = ['0-17', '18-25', '26-35', '36-45', '46-55', '56-65', '66-75', '76-85', '86-']
    age_ranges_conditions = [
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 0 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 17 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 18 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 25 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 26 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 35 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 36 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 45 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 46 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 55 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 56 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 65 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 66 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 75 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 76 \
      AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) <= 85 ',
      ' AND (Date_format(From_Days( To_Days(c.day) - To_Days(dob) ), \'%Y\' ) + 0) >= 86 '
    ]
    sub_queries = []
    sql_template = """
      SELECT
        :date_inserted AS date_inserted,
        :hpo_id AS hpo_id,
        (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
        c.day as date,
        '{0}' AS age_range,
        IFNULL((
          SELECT SUM(results.age_count)
          FROM
          (
            SELECT DATE(p.sign_up_time) AS day,
                   DATE(ps.date_of_birth) AS dob,
                   count(*) age_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
                   LEFT JOIN hpo ON p.hpo_id=hpo.hpo_id
            WHERE p.hpo_id = :hpo_id AND p.hpo_id <> :test_hpo_id
              AND p.is_ghost_id IS NOT TRUE
              AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
              AND p.withdrawal_status = :not_withdraw
              AND ps.date_of_birth IS NOT NULL
            GROUP BY DATE(p.sign_up_time), DATE(ps.date_of_birth)
          ) AS results
          WHERE results.day <= c.day {1}
        ),0) AS age_count
      FROM calendar c
      WHERE c.day BETWEEN :start_date AND :end_date
    """

    for age_range, age_range_condition in zip(age_ranges, age_ranges_conditions):
      sub_query = sql_template.format(age_range, age_range_condition)
      sub_queries.append(sub_query)

    sql += ' union '.join(sub_queries)

    return sql

class MetricsRaceCacheDao(BaseDao):

  def __init__(self):
    super(MetricsRaceCacheDao, self).__init__(MetricsRaceCache)

  def get_serving_version_with_session(self, session):
    return (session.query(MetricsRaceCache)
            .order_by(MetricsRaceCache.dateInserted.desc())
            .first())

  def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is None:
        return None
      last_inserted_date = last_inserted_record.dateInserted
      query = session.query(MetricsRaceCache)\
        .filter(MetricsRaceCache.dateInserted == last_inserted_date)
      if start_date:
        query = query.filter(MetricsRaceCache.date >= start_date)
      if end_date:
        query = query.filter(MetricsRaceCache.date <= end_date)

      if hpo_ids:
        query = query.filter(MetricsRaceCache.hpoId.in_(hpo_ids))

      return query.all()

  def delete_old_records(self, n_days_ago=7):
    with self.session() as session:
      last_inserted_record = self.get_serving_version_with_session(session)
      if last_inserted_record is not None:
        last_date_inserted = last_inserted_record.dateInserted
        seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
        delete_sql = """
          delete from metrics_race_cache where date_inserted < :seven_days_ago
        """
        params = {'seven_days_ago': seven_days_ago}
        session.execute(delete_sql, params)

  def to_client_json(self, result_set):
    client_json = []
    for record in result_set:
      new_item = {
        'date': record.date.isoformat(),
        'hpo': record.hpoName,
        'metrics': {
          'American_Indian_Alaska_Native': record.americanIndianAlaskaNative,
          'Asian': record.asian,
          'Black_African_American': record.blackAfricanAmerican,
          'Middle_Eastern_North_African': record.middleEasternNorthAfrican,
          'Native_Hawaiian_other_Pacific_Islander': record.nativeHawaiianOtherPacificIslander,
          'White': record.white,
          'Hispanic_Latino_Spanish': record.hispanicLatinoSpanish,
          'None_Of_These_Fully_Describe_Me': record.noneOfTheseFullyDescribeMe,
          'Prefer_Not_To_Answer': record.preferNotToAnswer,
          'Multi_Ancestry': record.multiAncestry,
          'No_Ancestry_Checked': record.noAncestryChecked
        }
      }
      client_json.append(new_item)
    return client_json

  def get_metrics_cache_sql(self):

    race_code_dict = {
      'Race_WhatRaceEthnicity': 193,
      'WhatRaceEthnicity_Hispanic': 207,
      'WhatRaceEthnicity_Black': 259,
      'WhatRaceEthnicity_White': 220,
      'WhatRaceEthnicity_AIAN': 252,
      'WhatRaceEthnicity_RaceEthnicityNoneOfThese': 235,
      'WhatRaceEthnicity_Asian': 194,
      'PMI_PreferNotToAnswer': 924,
      'WhatRaceEthnicity_MENA': 274,
      'PMI_Skip': 930,
      'WhatRaceEthnicity_NHPI': 237
    }

    for k in race_code_dict:
      code = CodeDao().get_code(PPI_SYSTEM, k)
      if code is not None:
        race_code_dict[k] = code.codeId

    sql = """
          insert into metrics_race_cache
            SELECT
              :date_inserted as date_inserted,
              hpo_id,
              name AS hpo_name,
              day,
              SUM(American_Indian_Alaska_Native) AS American_Indian_Alaska_Native,
              SUM(Asian) AS Asian,
              SUM(Black_African_American) AS Black_African_American,
              SUM(Middle_Eastern_North_African) AS Middle_Eastern_North_African,
              SUM(Native_Hawaiian_other_Pacific_Islander) AS Native_Hawaiian_other_Pacific_Islander,
              SUM(White) AS White,
              SUM(Hispanic_Latino_Spanish) AS Hispanic_Latino_Spanish,
              SUM(None_Of_These_Fully_Describe_Me) AS None_Of_These_Fully_Describe_Me,
              SUM(Prefer_Not_To_Answer) AS Prefer_Not_To_Answer,
              SUM(Multi_Ancestry) AS Multi_Ancestry,
              SUM(No_Ancestry_Checked) AS No_Ancestry_Checked
              FROM
              (
                SELECT p.hpo_id,
                       hpo.name,
                       day,
                       CASE WHEN WhatRaceEthnicity_AIAN=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS American_Indian_Alaska_Native,
                       CASE WHEN WhatRaceEthnicity_Asian=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Asian,
                       CASE WHEN WhatRaceEthnicity_Black=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Black_African_American,
                       CASE WHEN WhatRaceEthnicity_MENA=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Middle_Eastern_North_African,
                       CASE WHEN WhatRaceEthnicity_NHPI=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Native_Hawaiian_other_Pacific_Islander,
                       CASE WHEN WhatRaceEthnicity_White=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS White,
                       CASE WHEN WhatRaceEthnicity_Hispanic=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Hispanic_Latino_Spanish,
                       CASE WHEN WhatRaceEthnicity_RaceEthnicityNoneOfThese=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS None_Of_These_Fully_Describe_Me,
                       CASE WHEN PMI_PreferNotToAnswer=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Prefer_Not_To_Answer,
                       CASE
                         WHEN (WhatRaceEthnicity_Hispanic + WhatRaceEthnicity_Black + WhatRaceEthnicity_White + WhatRaceEthnicity_AIAN + WhatRaceEthnicity_Asian + WhatRaceEthnicity_MENA + WhatRaceEthnicity_NHPI) >
                              1
                           THEN 1
                         ELSE 0
                       END AS Multi_Ancestry,
                       CASE
                         WHEN (PMI_Skip = 1 AND Number_of_Answer=1) OR UNSET = 1
                           THEN 1
                         ELSE 0
                       END AS No_Ancestry_Checked
                FROM (
                       SELECT participant_id,
                              hpo_id,
                              sign_up_time,
                              MAX(WhatRaceEthnicity_Hispanic)                 AS WhatRaceEthnicity_Hispanic,
                              MAX(WhatRaceEthnicity_Black)                    AS WhatRaceEthnicity_Black,
                              MAX(WhatRaceEthnicity_White)                    AS WhatRaceEthnicity_White,
                              MAX(WhatRaceEthnicity_AIAN)                     AS WhatRaceEthnicity_AIAN,
                              MAX(UNSET)                                      AS UNSET,
                              MAX(WhatRaceEthnicity_RaceEthnicityNoneOfThese) AS WhatRaceEthnicity_RaceEthnicityNoneOfThese,
                              MAX(WhatRaceEthnicity_Asian)                    AS WhatRaceEthnicity_Asian,
                              MAX(PMI_PreferNotToAnswer)                      AS PMI_PreferNotToAnswer,
                              MAX(WhatRaceEthnicity_MENA)                     AS WhatRaceEthnicity_MENA,
                              MAX(PMI_Skip)                                   AS PMI_Skip,
                              MAX(WhatRaceEthnicity_NHPI)                     AS WhatRaceEthnicity_NHPI,
                              COUNT(*) as Number_of_Answer
                       FROM (
                              SELECT p.participant_id,
                                     p.hpo_id,
                                     p.sign_up_time,
                                     CASE WHEN q.code_id = {1} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Hispanic,
                                     CASE WHEN q.code_id = {2} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Black,
                                     CASE WHEN q.code_id = {3} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_White,
                                     CASE WHEN q.code_id = {4} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_AIAN,
                                     CASE WHEN q.code_id IS NULL THEN 1 ELSE 0 END AS UNSET,
                                     CASE WHEN q.code_id = {5} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_RaceEthnicityNoneOfThese,
                                     CASE WHEN q.code_id = {6} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Asian,
                                     CASE WHEN q.code_id = {7} THEN 1 ELSE 0 END   AS PMI_PreferNotToAnswer,
                                     CASE WHEN q.code_id = {8} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_MENA,
                                     CASE WHEN q.code_id = {9} THEN 1 ELSE 0 END   AS PMI_Skip,
                                     CASE WHEN q.code_id = {10} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_NHPI
                              FROM participant p
                                     INNER JOIN participant_summary ps ON p.participant_id = ps.participant_id
                                     LEFT JOIN
                                   (
                                     SELECT qr.participant_id, qra.value_code_id as code_id
                                     FROM questionnaire_question qq,
                                          questionnaire_response_answer qra,
                                          questionnaire_response qr
                                     WHERE qq.questionnaire_question_id = qra.question_id
                                       AND qq.code_id = {0}
                                       AND qra.questionnaire_response_id = qr.questionnaire_response_id
                                   ) q ON p.participant_id = q.participant_id
                              WHERE p.hpo_id=:hpo_id AND p.hpo_id <> :test_hpo_id
                                AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                                AND p.withdrawal_status = :not_withdraw
                                AND p.is_ghost_id IS NOT TRUE
                                AND ps.questionnaire_on_the_basics = 1
                            ) x
                       GROUP BY participant_id, hpo_id, sign_up_time
                     ) p,
                     calendar,
                     hpo
                WHERE p.hpo_id = hpo.hpo_id
                  AND calendar.day >= :start_date
                  AND calendar.day <= :end_date
                  AND calendar.day >= Date(p.sign_up_time)
              ) y
              GROUP BY day, hpo_id, name
              ;
        """.format(race_code_dict['Race_WhatRaceEthnicity'],
                   race_code_dict['WhatRaceEthnicity_Hispanic'],
                   race_code_dict['WhatRaceEthnicity_Black'],
                   race_code_dict['WhatRaceEthnicity_White'],
                   race_code_dict['WhatRaceEthnicity_AIAN'],
                   race_code_dict['WhatRaceEthnicity_RaceEthnicityNoneOfThese'],
                   race_code_dict['WhatRaceEthnicity_Asian'],
                   race_code_dict['PMI_PreferNotToAnswer'],
                   race_code_dict['WhatRaceEthnicity_MENA'],
                   race_code_dict['PMI_Skip'],
                   race_code_dict['WhatRaceEthnicity_NHPI'])
    return sql
