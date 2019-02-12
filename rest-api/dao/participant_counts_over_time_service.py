from werkzeug.exceptions import BadRequest
import datetime
from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, TEST_HPO_NAME, TEST_EMAIL_PATTERN
from participant_enums import WithdrawalStatus
from dao.hpo_dao import HPODao
from dao.base_dao import BaseDao
from dao.metrics_cache_dao import MetricsEnrollmentStatusCacheDao, MetricsGenderCacheDao, \
  MetricsAgeCacheDao, MetricsRaceCacheDao, MetricsRegionCacheDao, MetricsLifecycleCacheDao

CACHE_START_DATE = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()

class ParticipantCountsOverTimeService(BaseDao):

  def __init__(self):
    super(ParticipantCountsOverTimeService, self).__init__(ParticipantSummary, backup=True)
    self.test_hpo_id = HPODao().get_by_name(TEST_HPO_NAME).hpoId
    self.test_email_pattern = TEST_EMAIL_PATTERN

  def refresh_metrics_cache_data(self):

    self.refresh_data_for_metrics_cache(MetricsEnrollmentStatusCacheDao())
    self.refresh_data_for_metrics_cache(MetricsGenderCacheDao())
    self.refresh_data_for_metrics_cache(MetricsAgeCacheDao())
    self.refresh_data_for_metrics_cache(MetricsRaceCacheDao())
    self.refresh_data_for_metrics_cache(MetricsRegionCacheDao())
    self.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao())

  def refresh_data_for_metrics_cache(self, dao):
    updated_time = datetime.datetime.now()
    hpo_dao = HPODao()
    hpo_list = hpo_dao.get_all()
    for hpo in hpo_list:
      self.insert_cache_by_hpo(dao, hpo.hpoId, updated_time)

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

  def get_filtered_results(self, start_date, end_date, filters, filter_by, history,
                           stratification='ENROLLMENT_STATUS'):
    """Queries DB, returns results in format consumed by front-end

    :param start_date: Start date object
    :param end_date: End date object
    :param filters: Objects representing filters specified in UI
    :param filter_by: indicate how to filter the core participant
    :param history: query for history data from metrics cache table
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :return: Filtered, stratified results by date
    """

    # save the enrollment statuses requirements for filtering the SQL result later
    if 'enrollment_statuses' in filters and filters['enrollment_statuses'] is not None:
      enrollment_statuses = [str(val) for val in filters['enrollment_statuses']]
    else:
      enrollment_statuses = []

    if 'awardee_ids' in filters and filters['awardee_ids'] is not None:
      awardee_ids = filters['awardee_ids']
    else:
      awardee_ids = []

    # Filters for participant_summary (ps) and participant (p) table
    # filters_sql_ps is used in the general case when we're querying participant_summary
    # filters_sql_p is used when also LEFT OUTER JOINing p and ps
    filters_sql_ps = self.get_facets_sql(filters, stratification)
    filters_sql_p = self.get_facets_sql(filters, stratification, table_prefix='p')

    if str(history) == 'TRUE' and str(stratification) == 'TOTAL':
      dao = MetricsEnrollmentStatusCacheDao()
      return dao.get_total_interested_count(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and str(stratification) == 'ENROLLMENT_STATUS':
      dao = MetricsEnrollmentStatusCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and str(stratification) == 'GENDER_IDENTITY':
      dao = MetricsGenderCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and str(stratification) == 'AGE_RANGE':
      dao = MetricsAgeCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and str(stratification) == 'RACE':
      dao = MetricsRaceCacheDao()
      return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids)
    elif str(history) == 'TRUE' and str(stratification) in ['FULL_STATE', 'FULL_CENSUS',
                                                            'FULL_AWARDEE']:
      dao = MetricsRegionCacheDao()
      return dao.get_latest_version_from_cache(end_date, stratification, awardee_ids)
    elif str(history) == 'TRUE' and str(stratification) == 'LIFECYCLE':
      dao = MetricsLifecycleCacheDao()
      return dao.get_latest_version_from_cache(end_date, awardee_ids)
    elif str(stratification) == 'TOTAL':
      strata = ['TOTAL']
      sql = self.get_total_sql(filters_sql_ps)
    elif str(stratification) == 'ENROLLMENT_STATUS':
      strata = [str(val) for val in EnrollmentStatus]
      sql = self.get_enrollment_status_sql(filters_sql_p, filter_by)
    elif str(stratification) == 'EHR_CONSENT':
      strata = ['EHR_CONSENT']
      sql = self.get_total_sql(filters_sql_ps, ehr_count=True)
    elif str(stratification) == 'EHR_RATIO':
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
          if value is None or (str(stratification) == 'ENROLLMENT_STATUS'
                               and enrollment_statuses
                               and key not in enrollment_statuses):
            value = 0
          metrics[key] = int(value)
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
    if 'enrollment_statuses' in facets and str(stratification) == 'ENROLLMENT_STATUS':
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

  def get_total_sql(self, filters_sql, ehr_count=False):
    if ehr_count:
      # Participants with EHR Consent
      required_count = 'ps.consent_for_electronic_health_records = 1'
    else:
      # All participants
      required_count = '*'

    sql = """
        SELECT
            SUM(ps_sum.cnt * (ps_sum.day <= calendar.day)) registered_count,
            calendar.day start_date
        FROM calendar,
        (SELECT COUNT(%(count)s) cnt, DATE(p.sign_up_time) day
        FROM participant p
        LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
        %(filters)s
        GROUP BY day) ps_sum
        WHERE calendar.day >= :start_date
        AND calendar.day <= :end_date
        GROUP BY calendar.day
        ORDER BY calendar.day;
      """ % {'filters': filters_sql, 'count': required_count}

    return sql

  def get_ratio_sql(self, filters_sql):
    sql = """
        SELECT
            SUM(ps_sum.ratio * (ps_sum.day <= calendar.day)) ratio,
            calendar.day start_date
        FROM calendar,
        (SELECT avg(ps.consent_for_electronic_health_records = 1) ratio,
        DATE(p.sign_up_time) day
        FROM participant p
        LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
        %(filters)s
        GROUP BY day) ps_sum
        WHERE calendar.day >= :start_date
        AND calendar.day <= :end_date
        GROUP BY calendar.day
        ORDER BY calendar.day;
      """ % {'filters': filters_sql}

    return sql

  def get_enrollment_status_sql(self, filters_sql_p, filter_by='ORDERED'):

    # Noteworthy comments / documentation from Dan (and lightly adapted)
    #
    # This SQL works OK but hardcodes the baseline questionnaires and the
    # fact that 1ED04 and 1SAL are the samples needed to be a full
    # participant into the SQL. Previously this was done with config:
    #
    # rest-api/config/base_config.json#L29
    #
    # For the samples, MySQL doesn't make it easy to do the LEAST of N
    # nullable values, so this is probably OK...
    #
    # For the baseline questionnaires, note that we might want to use the config to
    # generate the GREATEST statement instead, but for now are hardcoding as
    # we do with samples.

    # TODO when implementing unit testing for service class:
    # Add macros for GREATEST and LEAST, as they don't work in SQLite
    # Example: master/rest-api/dao/database_utils.py#L50

    core_sample_time_field_name = 'enrollment_status_core_ordered_sample_time'
    if filter_by == 'STORED':
      core_sample_time_field_name = 'enrollment_status_core_stored_sample_time'

    sql = """
      SELECT
      SUM(CASE
        WHEN day>=Date(sign_up_time) AND (enrollment_status_member_time IS NULL OR day < Date(enrollment_status_member_time)) THEN 1
        ELSE 0
      END) AS registered_participants,
      sum(CASE
        WHEN enrollment_status_member_time IS NOT NULL AND day>=Date(enrollment_status_member_time) AND (%(core_sample_time_field_name)s IS NULL OR day < Date(%(core_sample_time_field_name)s)) THEN 1
        ELSE 0
      END) AS member_participants,
      sum(CASE
        WHEN %(core_sample_time_field_name)s IS NOT NULL AND day>=Date(%(core_sample_time_field_name)s) THEN 1
        ELSE 0
      END) AS full_participants,
      day
      FROM (SELECT p.sign_up_time, ps.enrollment_status_member_time, ps.enrollment_status_core_ordered_sample_time, ps.enrollment_status_core_stored_sample_time, calendar.day
            FROM participant p LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id,
                 calendar
            %(filters_p)s
              AND calendar.day >= :start_date
              AND calendar.day <= :end_date
           ) a
      GROUP BY day
      ORDER BY day;
      """ % {'filters_p': filters_sql_p, 'core_sample_time_field_name': core_sample_time_field_name}

    return sql
