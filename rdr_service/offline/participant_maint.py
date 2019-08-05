
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao

def skew_duplicate_last_modified():
  """
  Query participant summary looking for duplicate lastModified dates.
  If there are enough, randomly skew the milliseconds to eliminate
  the duplicates.
  """
  dao = ParticipantSummaryDao()
  min_dups = 6

  with dao.session() as session:

    # Find last modified dates from participant summary where there are
    # at least 6 duplicates.
    sql = """
      select last_modified, count from (
        select last_modified, count(1) as count 
          from participant_summary group by last_modified order by count desc
      ) a where a.count > :min_dups
    """
    results = session.execute(sql, {'min_dups': min_dups}).fetchall()

    if results and len(results) > 0:
      # loop over results and randomize only the microseconds value of the timestamp.
      for rec in results:
        sql = """
          update participant_summary set last_modified =
                date_add(date_format(last_modified, '%Y-%m-%d %H:%i:%S'), 
                         INTERVAL (FLOOR(RAND() * 999998) + 1) MICROSECOND)
             where last_modified = :ts
          """

        session.execute(sql, {'ts': rec['last_modified']})
