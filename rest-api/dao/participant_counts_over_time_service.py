from .participant_summary_dao import ParticipantSummaryDao
from model.participant_summary import ParticipantSummary

class ParticipantCountsOverTimeService(ParticipantSummaryDao):

  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary)


  def get_strata_by_filter(self, start_date, end_date, filters, stratification='TOTAL'):

    sql = """
      SELECT calendar.day start_date,
         {stratification},
         SUM(ps_sum.cnt * (ps_sum.day <= calendar.day)) cnt
      FROM calendar,
      (SELECT COUNT(*) cnt, ps.{stratification} {stratification}, DATE(ps.sign_up_time) day
            FROM participant_summary ps
          WHERE {filters}
          GROUP BY day) ps_sum,
      WHERE calendar.day >= :start_date
        AND calendar.day <= :end_date
      GROUP BY calendar.day, {stratification}
      ORDER BY calendar.day, {stratification};
    """.format(stratification=stratification, filters=filters)

    params = {'start_date': start_date, 'end_date': end_date}

    with self.session() as session:
      session.execute(sql, params)
