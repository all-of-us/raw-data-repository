from werkzeug.exceptions import BadRequest
import datetime
import logging
from model.participant_summary import ParticipantSummary
from model.metrics_cache import MetricsCacheJobStatus
from participant_enums import EnrollmentStatus, EnrollmentStatusV2, TEST_HPO_NAME, \
  TEST_EMAIL_PATTERN
from participant_enums import WithdrawalStatus, MetricsCacheType, Stratifications, MetricsAPIVersion
from dao.hpo_dao import HPODao
from dao.base_dao import BaseDao
from dao.metrics_cache_dao import MetricsEnrollmentStatusCacheDao, MetricsGenderCacheDao, \
  MetricsAgeCacheDao, MetricsRaceCacheDao, MetricsRegionCacheDao, MetricsLifecycleCacheDao, \
  MetricsLanguageCacheDao, MetricsCacheJobStatusDao

CACHE_START_DATE = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()

class ParticipantCountsOverTimeService(BaseDao):

  def __init__(self):
    super(ParticipantCountsOverTimeService, self).__init__(ParticipantSummary, backup=True)
    self.test_hpo_id = HPODao().get_by_name(TEST_HPO_NAME).hpoId
    self.test_email_pattern = TEST_EMAIL_PATTERN

  def init_tmp_table(self):
    dao = MetricsCacheJobStatusDao()
    dao.init_tmp_table()
    logging.info('Init tmp table for metrics cron job.')

  def refresh_metrics_cache_data(self):
    self.refresh_data_for_metrics_cache(MetricsEnrollmentStatusCacheDao())
    logging.info('Refresh MetricsEnrollmentStatusCache done.')
    self.refresh_data_for_metrics_cache(MetricsGenderCacheDao(MetricsCacheType.METRICS_V2_API))
    logging.info('Refresh MetricsGenderCache for Metrics2API done.')
    self.refresh_data_for_metrics_cache(MetricsGenderCacheDao(
      MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
    logging.info('Refresh MetricsGenderCache for Public Metrics API done.')
    self.refresh_data_for_metrics_cache(MetricsAgeCacheDao(MetricsCacheType.METRICS_V2_API))
    logging.info('Refresh MetricsAgeCache for Metrics2API done.')
    self.refresh_data_for_metrics_cache(MetricsAgeCacheDao(
      MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
    logging.info('Refresh MetricsAgeCache for Public Metrics API done.')
    self.refresh_data_for_metrics_cache(MetricsRaceCacheDao(MetricsCacheType.METRICS_V2_API))
    logging.info('Refresh MetricsRaceCache for Metrics2API done.')
    self.refresh_data_for_metrics_cache(MetricsRaceCacheDao(
      MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
    logging.info('Refresh MetricsRaceCache for Public Metrics API done.')
    self.refresh_data_for_metrics_cache(MetricsRegionCacheDao())
    logging.info('Refresh MetricsRegionCache done.')
    self.refresh_data_for_metrics_cache(MetricsLanguageCacheDao())
    logging.info('Refresh MetricsLanguageCache done.')
    self.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType.METRICS_V2_API))
    logging.info('Refresh MetricsLifecycleCache for Metrics2API done.')
    self.refresh_data_for_metrics_cache(
      MetricsLifecycleCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
    logging.info('Refresh MetricsLifecycleCache for Public Metrics API done.')

  def refresh_data_for_metrics_cache(self, dao):
    status_dao = MetricsCacheJobStatusDao()
    updated_time = datetime.datetime.now()
    kwargs = dict(
      cacheTableName=dao.table_name,
      type=str(dao.cache_type),
      inProgress=True,
      complete=False,
      dateInserted=updated_time
    )
    job_status_obj = MetricsCacheJobStatus(**kwargs)
    status_obj = status_dao.insert(job_status_obj)

    hpo_dao = HPODao()
    hpo_list = hpo_dao.get_all()
    for hpo in hpo_list:
      self.insert_cache_by_hpo(dao, hpo.hpoId, updated_time)

    status_dao.set_to_complete(status_obj)
    dao.delete_old_records()

  def insert_cache_by_hpo(self, dao, hpo_id, updated_time):
    sql = dao.get_metrics_cache_sql()
    start_date = CACHE_START_DATE
    end_date = datetime.datetime.now().date() + datetime.timedelta(days=10)

    params = {'hpo_id': hpo_id, 'test_hpo_id': self.test_hpo_id,
              'not_withdraw': int(WithdrawalStatus.NOT_WITHDRAWN),
              'test_email_pattern': self.test_email_pattern, 'start_date': start_date,
              'end_date': end_date, 'date_inserted': updated_time}
    with dao.session() as session:
      session.execute(sql, params)

  def get_filtered_results(self, stratification, start_date, end_date, history, awardee_ids,
                           enrollment_statuses, sample_time_def, version):
    """Queries DB, returns results in format consumed by front-end

    :param start_date: Start date object
    :param end_date: End date object
    :param awardee_ids: indicate awardee ids
    :param enrollment_statuses: indicate the enrollment status
    :param sample_time_def: indicate how to filter the core participant
    :param history: query for history data from metrics cache table
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :param version: indicate the version of the result filter
    :return: Filtered, stratified results by date
    """

    # Filters for participant_summary (ps) and participant (p) table
    # filters_sql_ps is used in the general case when we're querying participant_summary
    # filters_sql_p is used when also LEFT OUTER JOINing p and ps
    facets = {
      'enrollment_statuses': [EnrollmentStatusV2(val)
                              if version == MetricsAPIVersion.V2 else EnrollmentStatus(val)
                              for val in enrollment_statuses],
      'awardee_ids': awardee_ids
    }
    filters_sql_ps = self.get_facets_sql(facets, stratification)
    filters_sql_p = self.get_facets_sql(facets, stratification, table_prefix='p')

    if str(history) == 'TRUE' and stratification == Stratifications.TOTAL:
      dao = MetricsEnrollmentStatusCacheDao()
      return dao.get_total_interested_count(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and stratification == Stratifications.ENROLLMENT_STATUS:
      dao = MetricsEnrollmentStatusCacheDao(version=version)
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and stratification == Stratifications.GENDER_IDENTITY:
      dao = MetricsGenderCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids,
                                               enrollment_statuses)
    elif str(history) == 'TRUE' and stratification == Stratifications.AGE_RANGE:
      dao = MetricsAgeCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids,
                                               enrollment_statuses)
    elif str(history) == 'TRUE' and stratification == Stratifications.RACE:
      dao = MetricsRaceCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids,
                                               enrollment_statuses)
    elif str(history) == 'TRUE' and stratification in [Stratifications.FULL_STATE,
                                                       Stratifications.FULL_CENSUS,
                                                       Stratifications.FULL_AWARDEE,
                                                       Stratifications.GEO_STATE,
                                                       Stratifications.GEO_CENSUS,
                                                       Stratifications.GEO_AWARDEE]:
      dao = MetricsRegionCacheDao(version=version)
      return dao.get_latest_version_from_cache(end_date, stratification, awardee_ids,
                                               enrollment_statuses)
    elif str(history) == 'TRUE' and stratification == Stratifications.LANGUAGE:
      dao = MetricsLanguageCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids,
                                               enrollment_statuses)
    elif str(history) == 'TRUE' and stratification == Stratifications.LIFECYCLE:
      dao = MetricsLifecycleCacheDao(version=version)
      return dao.get_latest_version_from_cache(end_date, awardee_ids)
    elif stratification == Stratifications.TOTAL:
      strata = ['TOTAL']
      sql = self.get_total_sql(filters_sql_ps)
    elif version == MetricsAPIVersion.V2 and stratification == Stratifications.ENROLLMENT_STATUS:
      strata = [str(val) for val in EnrollmentStatusV2]
      sql = self.get_enrollment_status_sql(filters_sql_p, sample_time_def, version)
    elif stratification == Stratifications.ENROLLMENT_STATUS:
      strata = [str(val) for val in EnrollmentStatus]
      sql = self.get_enrollment_status_sql(filters_sql_p, sample_time_def)
    elif stratification == Stratifications.EHR_CONSENT:
      strata = ['EHR_CONSENT']
      sql = self.get_total_sql(filters_sql_ps, ehr_count=True)
    elif stratification == Stratifications.EHR_RATIO:
      strata = ['EHR_RATIO']
      sql = self.get_ratio_sql(filters_sql_ps)
    else:
      raise BadRequest('Invalid stratification: %s' % stratification)

    params = {'start_date': start_date, 'end_date': end_date}

    results_by_date = []

    with self.session() as session:
      cursor = session.execute(sql, params)

    # Iterate through each result (by date), transforming tabular SQL results
    # into expected list-of-dictionaries response format
    try:
      results = cursor.fetchall()
      for result in results:
        date = result[-1]
        metrics = {}
        values = result[:-1]
        for i, value in enumerate(values):
          key = strata[i]
          if value is None or (stratification == Stratifications.ENROLLMENT_STATUS
                               and enrollment_statuses
                               and key not in enrollment_statuses):
            value = 0
          metrics[key] = float(value) if stratification == Stratifications.EHR_RATIO else int(value)
        results_by_date.append({
          'date': str(date),
          'metrics': metrics
        })
    finally:
      cursor.close()

    return results_by_date

  def get_facets_sql(self, facets, stratification, table_prefix='ps'):
    """Helper function to transform facets/filters selection into SQL

    :param facets: Object representing facets and filters to apply to query results
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :param table_prefix: Either 'ps' (for participant_summary) or 'p' (for participant)
    :return: SQL for 'WHERE' clause, reflecting filters specified in UI
    """

    facets_sql = 'WHERE '
    facets_sql_list = []

    facet_map = {
      'awardee_ids': 'hpo_id',
      'enrollment_statuses': 'enrollment_status'
    }

    # the SQL for ENROLLMENT_STATUS stratify is using the enrollment status time
    # instead of enrollment status
    if 'enrollment_statuses' in facets and stratification == Stratifications.ENROLLMENT_STATUS:
      del facets['enrollment_statuses']
      del facet_map['enrollment_statuses']

    for facet in facets:
      filter_prefix = table_prefix
      filters_sql = []
      db_field = facet_map[facet]
      filters = facets[facet]

      allow_null = False
      if db_field == 'enrollment_status':
        filter_prefix = 'ps'
        allow_null = True

      # TODO:
      # Consider using an IN clause with bound parameters, instead, which
      # would be simpler than this,
      #
      # TODO:
      # Consider using bound parameters here instead of inlining the values
      # in the SQL. We do that in other places using this function:
      #
      # dao/database_utils.py#L16
      #
      # This may help the SQL perform slightly better since the execution
      # plan for the query can be cached when the only thing changing are
      # the bound params.
      for q_filter in filters:
        if str(q_filter) != '':
          filter_sql = filter_prefix + '.' + db_field + ' = ' + str(int(q_filter))
          if allow_null and str(int(q_filter)) == '1':
            filters_sql.append('(' + filter_sql + ' or ' + filter_prefix
                               + '.' + db_field + ' IS NULL)')
          else:
            filters_sql.append(filter_sql)
      if len(filters_sql) > 0:
        filters_sql = '(' + ' OR '.join(filters_sql) + ')'
        facets_sql_list.append(filters_sql)

    if len(facets_sql_list) > 0:
      facets_sql += ' AND '.join(facets_sql_list) + ' AND'

    # TODO: use bound parameters
    # See https://github.com/all-of-us/raw-data-repository/pull/669/files/a08be0ffe445da60ebca13b41d694368e4d42617#diff-6c62346e0cbe4a7fd7a45af6d4559c3e  # pylint: disable=line-too-long
    facets_sql += ' %(table_prefix)s.hpo_id != %(test_hpo_id)s ' % {
      'table_prefix': table_prefix, 'test_hpo_id': self.test_hpo_id}
    facets_sql += ' AND (ps.email IS NULL OR NOT ps.email LIKE "%(test_email_pattern)s")' % {
      'test_email_pattern': self.test_email_pattern}
    facets_sql += ' AND %(table_prefix)s.withdrawal_status = %(not_withdrawn)i' % {
      'table_prefix': table_prefix, 'not_withdrawn': WithdrawalStatus.NOT_WITHDRAWN}
    facets_sql += ' AND p.is_ghost_id IS NOT TRUE '

    return facets_sql

  @staticmethod
  def get_total_sql(filters_sql, ehr_count=False):
    if ehr_count:
      # date consented
      date_field = 'ps.consent_for_electronic_health_records_time'
    else:
      # date joined
      date_field = 'p.sign_up_time'

    return """
        SELECT
          SUM(ps_sum.cnt * (ps_sum.day <= calendar.day)) registered_count,
          calendar.day start_date
        FROM calendar,
        (
          SELECT
            COUNT(*) cnt,
            DATE(%(date_field)s) day
          FROM participant p
          LEFT OUTER JOIN participant_summary ps
            ON p.participant_id = ps.participant_id
          %(filters)s
          GROUP BY day
        ) ps_sum
        WHERE calendar.day >= :start_date
        AND calendar.day <= :end_date
        GROUP BY calendar.day
        ORDER BY calendar.day;
      """ % {'filters': filters_sql, 'date_field': date_field}

  @staticmethod
  def get_ratio_sql(filters_sql):
    return """
      select
        ifnull(
          (
            select count(*)
            from participant p
            LEFT OUTER JOIN participant_summary ps
              ON p.participant_id = ps.participant_id
            %(filters)s
              and ps.consent_for_electronic_health_records_time <= calendar.day
          ) / (
            select count(*)
            from participant p
            LEFT OUTER JOIN participant_summary ps
              ON p.participant_id = ps.participant_id
            %(filters)s
              and p.sign_up_time <= calendar.day
          ),
          0
        ) ratio,
        calendar.day start_date
      from calendar
      where calendar.day >= :start_date
        and calendar.day <= :end_date
      order by calendar.day;
    """ % {'filters': filters_sql}

  def get_enrollment_status_sql(self, filters_sql_p, filter_by='ORDERED', version=None):

    core_sample_time_field_name = 'enrollment_status_core_ordered_sample_time'
    if filter_by == 'STORED':
      core_sample_time_field_name = 'enrollment_status_core_stored_sample_time'

    if version == MetricsAPIVersion.V2:
      sql = """
        SELECT
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(p.sign_up_time) AS sign_up_time,
                   DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(p.sign_up_time), DATE(ps.consent_for_study_enrollment_time)
          ) AS results
          WHERE c.day>=DATE(sign_up_time) AND consent_for_study_enrollment_time IS NULL
        ),0) AS registered,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                   DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.consent_for_study_enrollment_time), DATE(ps.enrollment_status_member_time)
          ) AS results
          WHERE consent_for_study_enrollment_time IS NOT NULL AND c.day>=DATE(consent_for_study_enrollment_time) AND (enrollment_status_member_time IS NULL OR c.day < DATE(enrollment_status_member_time))
        ),0) AS participant,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.enrollment_status_member_time), DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE enrollment_status_member_time IS NOT NULL AND day>=DATE(enrollment_status_member_time) AND (%(core_sample_time_field_name)s IS NULL OR day < DATE(%(core_sample_time_field_name)s))
        ),0) AS fully_consented,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE %(core_sample_time_field_name)s IS NOT NULL AND day>=DATE(%(core_sample_time_field_name)s)
        ),0) AS core_participant,
        day
        FROM calendar c
        WHERE c.day BETWEEN :start_date AND :end_date
        """ % {'filters_p': filters_sql_p, 'core_sample_time_field_name': core_sample_time_field_name}
    else:
      sql = """
        SELECT
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(p.sign_up_time) AS sign_up_time,
                   DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(p.sign_up_time), DATE(ps.enrollment_status_member_time)
          ) AS results
          WHERE c.day>=DATE(sign_up_time) AND (enrollment_status_member_time IS NULL OR c.day < DATE(enrollment_status_member_time))
        ),0) AS registered_participants,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.enrollment_status_member_time), DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE enrollment_status_member_time IS NOT NULL AND day>=DATE(enrollment_status_member_time) AND (%(core_sample_time_field_name)s IS NULL OR day < DATE(%(core_sample_time_field_name)s))
        ),0) AS member_participants,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE %(core_sample_time_field_name)s IS NOT NULL AND day>=DATE(%(core_sample_time_field_name)s)
        ),0) AS full_participants,
        day
        FROM calendar c
        WHERE c.day BETWEEN :start_date AND :end_date
        """ % {'filters_p': filters_sql_p, 'core_sample_time_field_name': core_sample_time_field_name}

    return sql
