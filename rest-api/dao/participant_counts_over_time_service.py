from .participant_summary_dao import ParticipantSummaryDao
from model.participant_summary import ParticipantSummary

class ParticipantCountsOverTimeService(ParticipantSummaryDao):

  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary)


  def get_strata_by_filter(self, start_date, end_date, filters, stratification='ENROLLMENT_STATUS'):

    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')

    filters_sql = self.get_facets_sql(filters)

    if stratification == 'TOTAL':
      sql = """
        SELECT calendar.day start_date,
            SUM(ps_sum.cnt * (ps_sum.day <= calendar.day)) registered_count
        FROM calendar,
        (SELECT COUNT(*) cnt, DATE(ps.sign_up_time) day
        FROM participant_summary ps
        GROUP BY day) ps_sum
        WHERE calendar.day >= :start_date
        AND calendar.day <= :end_date
        GROUP BY calendar.day
        ORDER BY calendar.day;
      """
    elif stratification == 'ENROLLMENT_STATUS':
      sql = """
      SELECT SUM(registered_cnt * (cnt_day <= calendar.day)) registered_participants,
       SUM(member_cnt * (cnt_day <= calendar.day)) member_participants,
       SUM(full_cnt * (cnt_day <= calendar.day)) full_participants,
       calendar.day from
       (SELECT c2.day cnt_day,
               registered.cnt registered_cnt,
               member.cnt member_cnt,
               full.cnt full_cnt
            FROM calendar c2
            LEFT OUTER JOIN
            (SELECT COUNT(*) cnt, DATE(ps.consent_for_study_enrollment_time) day
               FROM participant_summary ps
              WHERE :filters
              GROUP BY day) registered
             ON c2.day = registered.day
           LEFT OUTER JOIN
            (SELECT COUNT(*) cnt,
                    DATE(ps.consent_for_electronic_health_records_time) day
               FROM participant_summary ps
              WHERE :filters
           GROUP BY day) member
             ON c2.day = member.day
           LEFT OUTER JOIN
            (SELECT COUNT(*) cnt,
             DATE(CASE WHEN enrollment_status = 3 THEN
                   GREATEST(consent_for_electronic_health_records,
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
              WHERE :filters
           GROUP BY day) full
             ON c2.day = full.day) day_sums, calendar
          WHERE calendar.day >= :start_date
            AND calendar.day <= :end_date
          GROUP BY calendar.day
          ORDER BY calendar.day;
      """

    params = {'start_date': start_date, 'end_date': end_date, 'filters': filters_sql}

    results_by_date = []

    with self.session() as session:
      cursor = session.execute(sql, params)

    try:
      results = cursor.fetchall()
      for result in results:
        results_by_date.append(result)
    finally:
      cursor.close()

      return results_by_date


  def get_facets_sql(self, facets):

    facets_sql = []

    facet_map = {
      'awardee_ids': 'hpo_id',
      'withdrawal_status': 'withdrawal_status',
      'enrollment_statuses': 'enrollment_status'
    }

    for facet in facets:
      filters_sql = []
      db_field = facet_map[facet]
      filters = facets[facet]
      for filter in filters:
        if str(filter) != '':
          filters_sql.append('ps.' + db_field + ' = ' + str(int(filter)))
      if len(filters_sql) > 0:
        filters_sql = ' OR '.join(filters_sql)
        facets_sql.append(filters_sql)

    if len(facets_sql) > 0:
      facets_sql = ' AND '.join(facets_sql)
    else:
      facets_sql = ''

    return facets_sql

