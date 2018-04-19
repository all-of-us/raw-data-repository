from werkzeug.exceptions import BadRequest

from .participant_summary_dao import ParticipantSummaryDao
from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, TEST_HPO_NAME, TEST_EMAIL_PATTERN
from participant_enums import WithdrawalStatus
from dao.hpo_dao import HPODao

class ParticipantCountsOverTimeService(ParticipantSummaryDao):

  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary)

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

    if str(stratification) == 'TOTAL':
      strata = ['TOTAL']
      sql = self.get_total_sql(filters_sql_ps)
    elif str(stratification) == 'ENROLLMENT_STATUS':
      strata = [str(val) for val in EnrollmentStatus]
      sql = self.get_enrollment_status_sql(filters_sql_ps, filters_sql_p)
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
          if value == None:
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
      filters_sql = []
      db_field = facet_map[facet]
      filters = facets[facet]

      if db_field == 'enrollment_status':
        table_prefix = 'ps'

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
          filters_sql.append(table_prefix + '.' + db_field + ' = ' + str(int(q_filter)))
      if len(filters_sql) > 0:
        filters_sql = '(' + ' OR '.join(filters_sql) + ')'
        facets_sql_list.append(filters_sql)

    if len(facets_sql_list) > 0:
      facets_sql += ' AND '.join(facets_sql_list) + ' AND'

    facets_sql += ' %(table_prefix)s.hpo_id != %(test_hpo_id)s ' % {
      'table_prefix': table_prefix, 'test_hpo_id': self.test_hpo_id}
    facets_sql += ' AND NOT ps.email LIKE "%(test_email_pattern)s"' % {
      'test_email_pattern': self.test_email_pattern}
    facets_sql += ' AND ps.withdrawal_status = %(not_withdrawn)i' % {
      'not_withdrawn': WithdrawalStatus.NOT_WITHDRAWN}

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

    sql = """
      SELECT
         SUM(registered_cnt * (cnt_day <= calendar.day)) registered_participants,
         SUM(member_cnt * (cnt_day <= calendar.day)) member_participants,
         SUM(full_cnt * (cnt_day <= calendar.day)) full_participants,
         calendar.day
       FROM
       (SELECT c2.day cnt_day,
               registered.cnt registered_cnt,
               member.cnt member_cnt,
               full.cnt full_cnt
            FROM calendar c2
            LEFT OUTER JOIN
            (SELECT COUNT(*) cnt,
                (CASE WHEN enrollment_status = 1 THEN
                  DATE(p.sign_up_time)
                  ELSE NULL END) day
                FROM participant p
                LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
              %(filters_p)s
              GROUP BY day) registered
             ON c2.day = registered.day
           LEFT OUTER JOIN
            (SELECT COUNT(*) cnt,
                   (CASE WHEN enrollment_status = 2 THEN
                    DATE(ps.consent_for_electronic_health_records_time)
                    ELSE NULL END) day
               FROM participant_summary ps
              %(filters_ps)s
           GROUP BY day) member
             ON c2.day = member.day
           LEFT OUTER JOIN
            (SELECT COUNT(*) cnt,
             DATE(CASE WHEN enrollment_status = 3 THEN
                   GREATEST(consent_for_electronic_health_records_time,
                            questionnaire_on_the_basics_time,
                            questionnaire_on_lifestyle_time,
                            questionnaire_on_overall_health_time,
                            physical_measurements_time,
                            CASE WHEN sample_status_1ed04_time IS NOT NULL
                             THEN
                             (CASE WHEN sample_status_1sal_time IS NOT
                                NULL
                                THEN LEAST(sample_status_1ed04_time,
                                           sample_status_1sal_time)
                                ELSE sample_status_1ed04_time END)
                             ELSE sample_status_1sal_time END)
                   ELSE NULL END) day
               FROM participant_summary ps
              %(filters_ps)s
           GROUP BY day) full
             ON c2.day = full.day) day_sums, calendar
          WHERE calendar.day >= :start_date
            AND calendar.day <= :end_date
          GROUP BY calendar.day
          ORDER BY calendar.day;
      """ % {'filters_ps': filters_sql_ps, 'filters_p': filters_sql_p}

    return sql
