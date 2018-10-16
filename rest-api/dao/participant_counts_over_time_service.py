from werkzeug.exceptions import BadRequest

from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, TEST_HPO_NAME, TEST_EMAIL_PATTERN
from participant_enums import WithdrawalStatus
from dao.hpo_dao import HPODao
from dao.base_dao import BaseDao

class ParticipantCountsOverTimeService(BaseDao):

  def __init__(self):
    super(ParticipantCountsOverTimeService, self).__init__(ParticipantSummary, backup=True)

  def get_filtered_results(self, start_date, end_date, filters, stratification='ENROLLMENT_STATUS'):
    """Queries DB, returns results in format consumed by front-end

    :param start_date: Start date object
    :param end_date: End date object
    :param filters: Objects representing filters specified in UI
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :return: Filtered, stratified results by date
    """

    self.test_hpo_id = HPODao().get_by_name(TEST_HPO_NAME).hpoId
    self.test_email_pattern = TEST_EMAIL_PATTERN

    # Filters for participant_summary (ps) and participant (p) table
    # filters_sql_ps is used in the general case when we're querying participant_summary
    # filters_sql_p is used when also LEFT OUTER JOINing p and ps
    filters_sql_ps = self.get_facets_sql(filters)
    filters_sql_p = self.get_facets_sql(filters, table_prefix='p')

    # check if request stratification is for enrollment status
    query_for_enrollment_status = False

    target_enrollment_statuses = []
    if 'enrollment_statuses' in filters:
      target_enrollment_statuses = [str(val) for val in filters['enrollment_statuses']]

    if str(stratification) == 'TOTAL':
      strata = ['TOTAL']
      sql = self.get_total_sql(filters_sql_ps)
    elif str(stratification) == 'ENROLLMENT_STATUS':
      query_for_enrollment_status = True
      strata = [str(val) for val in EnrollmentStatus]
      sql = self.get_enrollment_status_sql(filters_sql_ps, filters_sql_p)
    elif str(stratification) == 'ENROLLMENT_STATUS_BY_ORDER_TIME':
      query_for_enrollment_status = True
      strata = [str(val) for val in EnrollmentStatus]
      sql = self.get_enrollment_status_by_order_time_sql(filters_sql_ps, filters_sql_p)
    else:
      raise BadRequest('Invalid stratification: %s' % stratification)

    params = {'start_date': start_date, 'end_date': end_date}

    results_by_date = []

    with self.session() as session:
      cursor = session.execute(sql, params)

    # Iterate through each result (by date), transforming tabular SQL results
    # into expected list-of-dictionaries response format
    # set value = 0 if request stratification is for enrollment status
    # but key is not in target_enrollment_statuses
    try:
      results = cursor.fetchall()
      for result in results:
        date = result[-1]
        metrics = {}
        values = result[:-1]
        for i, value in enumerate(values):
          key = strata[i]
          if value is None or (query_for_enrollment_status
                               and 'enrollment_statuses' in filters
                               and key not in target_enrollment_statuses):
            value = 0
          metrics[key] = int(value)
        results_by_date.append({
          'date': str(date),
          'metrics': metrics
        })
    finally:
      cursor.close()

    return results_by_date

  def get_facets_sql(self, facets, table_prefix='ps'):
    """Helper function to transform facets/filters selection into SQL

    :param facets: Object representing facets and filters to apply to query results
    :param table_prefix: Either 'ps' (for participant_summary) or 'p' (for participant)
    :return: SQL for 'WHERE' clause, reflecting filters specified in UI
    """

    facets_sql = 'WHERE'
    facets_sql_list = []

    facet_map = {
      'awardee_ids': 'hpo_id',
      'enrollment_statuses': 'enrollment_status'
    }

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


    return facets_sql

  def get_total_sql(self, filters_sql):

    sql = """
        SELECT
            SUM(ps_sum.cnt * (ps_sum.day <= calendar.day)) registered_count,
            calendar.day start_date
        FROM calendar,
        (SELECT COUNT(*) cnt, DATE(p.sign_up_time) day
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

  def get_enrollment_status_sql(self, filters_sql_ps, filters_sql_p):

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

    sql_by_sample_status_time = """
      select sum(is_register) as registered_participants,
       sum(is_member) as member_participants,
       sum(is_core) as full_participants,
       z.day
      from
      (
           select
            x.participant_id,
            case when register_day<=y.day and member_day>y.day then 1 else 0 end as is_register,
            case when member_day<=y.day and core_day > y.day then 1 else 0 end as is_member,
            case when core_day<=y.day then 1 else 0 end as is_core,
            y.day
          from
          (select a.participant_id,a.register_day,COALESCE(b.member_day,'3000-01-01') member_day,COALESCE(b.core_day,'3000-01-01') core_day from
                (SELECT p.participant_id,
                       COALESCE(p.sign_up_time, '3000-01-01') as register_day
                FROM participant p
                       LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
                %(filters_p)s) a left join
               (select participant_id,
                      (
                          CASE
                            WHEN enrollment_status>=2 and ps.consent_for_electronic_health_records = 1 THEN DATE(COALESCE(ps.consent_for_electronic_health_records_time, '3000-01-01'))
                            ELSE Date('3000-01-01') END
                          ) member_day,
                       case when enrollment_status=3 then
                       DATE(
                         GREATEST(
                           GREATEST(
                             COALESCE(sign_up_time, '1000-01-01'),
                             COALESCE(consent_for_electronic_health_records_time, '1000-01-01'),
                             COALESCE(questionnaire_on_the_basics_time, '1000-01-01'),
                             COALESCE(questionnaire_on_lifestyle_time, '1000-01-01'),
                             COALESCE(questionnaire_on_overall_health_time, '1000-01-01'),
                             COALESCE(physical_measurements_finalized_time, '1000-01-01')
                               ),
                           LEAST(
                             COALESCE(sample_status_1ed04_time, '3000-01-01'),
                             COALESCE(sample_status_2ed10_time, '3000-01-01'),
                             COALESCE(sample_status_1ed10_time, '3000-01-01'),
                             COALESCE(sample_status_1sal_time, '3000-01-01'),
                             COALESCE(sample_status_1sal2_time, '3000-01-01')
                               ))
                       )
                       else
                           Date('3000-01-01')
                       end core_day
               from participant_summary ps
               %(filters_ps)s) b
               on b.participant_id=a.participant_id
          where register_day<=:end_date or register_day='3000-01-01'
          ) x join
          (
           select * from calendar
           WHERE calendar.day >= :start_date
           AND calendar.day <= :end_date
          ) y
      ) z
      group by day;
    """ % {'filters_ps': filters_sql_ps, 'filters_p': filters_sql_p}

    return sql_by_sample_status_time


  def get_enrollment_status_by_order_time_sql(self, filters_sql_ps, filters_sql_p):

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

    sql_by_sample_order_status_time = """
      select sum(is_register) as registered_participants,
             sum(is_member) as member_participants,
             sum(is_core) as full_participants,
             z.day
      from
      (
           select
            x.participant_id,
            case when register_day<=y.day and member_day>y.day then 1 else 0 end as is_register,
            case when member_day<=y.day and core_day > y.day then 1 else 0 end as is_member,
            case when core_day<=y.day then 1 else 0 end as is_core,
            y.day
          from
          (select a.participant_id,a.register_day,COALESCE(b.member_day,'3000-01-01') member_day,COALESCE(b.core_day,'3000-01-01') core_day from
                (SELECT p.participant_id,
                       COALESCE(p.sign_up_time, '3000-01-01') as register_day
                FROM participant p
                       LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
                %(filters_p)s) a left join
               (select participant_id,
                      (
                          CASE
                            WHEN enrollment_status>=2 and ps.consent_for_electronic_health_records = 1 THEN DATE(COALESCE(ps.consent_for_electronic_health_records_time, '3000-01-01'))
                            ELSE Date('3000-01-01') END
                          ) member_day,
                       case when
                           enrollment_status>=2
                           and consent_for_electronic_health_records_time is not null
                           and questionnaire_on_the_basics_time is not null
                           and questionnaire_on_lifestyle_time is not null
                           and questionnaire_on_overall_health_time is not null
                           and physical_measurements_finalized_time is not null
                           then
                       DATE(
                         GREATEST(
                           GREATEST(
                             COALESCE(sign_up_time, '1000-01-01'),
                             COALESCE(consent_for_electronic_health_records_time, '1000-01-01'),
                             COALESCE(questionnaire_on_the_basics_time, '1000-01-01'),
                             COALESCE(questionnaire_on_lifestyle_time, '1000-01-01'),
                             COALESCE(questionnaire_on_overall_health_time, '1000-01-01'),
                             COALESCE(physical_measurements_finalized_time, '1000-01-01')
                               ),
                           LEAST(
                             COALESCE(sample_order_status_1ed04_time, '3000-01-01'),
                             COALESCE(sample_order_status_2ed10_time, '3000-01-01'),
                             COALESCE(sample_order_status_1ed10_time, '3000-01-01'),
                             COALESCE(sample_order_status_1sal_time, '3000-01-01'),
                             COALESCE(sample_order_status_1sal2_time, '3000-01-01')
                               ))
                       )
                       else
                           Date('3000-01-01')
                       end core_day
               from participant_summary ps
               %(filters_ps)s) b
               on b.participant_id=a.participant_id
          where register_day<=:end_date or register_day='3000-01-01'
          ) x join
          (
           select * from calendar
           WHERE calendar.day >= :start_date
           AND calendar.day <= :end_date
          ) y
      ) z
      group by day;
    """ % {'filters_ps': filters_sql_ps, 'filters_p': filters_sql_p}

    return sql_by_sample_order_status_time
