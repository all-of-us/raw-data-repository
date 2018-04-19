import clock
import collections

from model.metric_set import AggregateMetrics, MetricSet
from sqlalchemy import text
from dao import database_factory
from dao.database_utils import replace_years_old
from dao.hpo_dao import HPODao
from dao.metric_set_dao import AggregateMetricsDao, MetricSetDao
from participant_enums import MetricSetType, MetricsKey
from participant_enums import EnrollmentStatus, OrderStatus, PhysicalMeasurementsStatus
from participant_enums import Race, QuestionnaireStatus, WithdrawalStatus
from participant_enums import TEST_EMAIL_PATTERN, TEST_HPO_NAME


LIVE_METRIC_SET_ID = 'public-agg.live'

def _questionnaire_metric(name, col):
  """Returns a metrics SQL aggregation tuple for the given key/column."""
  return _SqlAggregation(
      name,
      """
      SELECT {col}, COUNT(*)
      FROM participant_summary
      WHERE {summary_filter_sql}
      GROUP BY 1;
      """.format(col=col, summary_filter_sql=_SUMMARY_FILTER_SQL),
      lambda v: QuestionnaireStatus.lookup_by_number(v).name,
      None
  )

_SUMMARY_FILTER_SQL = """
(withdrawal_status = :not_withdrawn_status
 AND NOT participant_summary.email LIKE :test_email_pattern
 AND NOT participant_summary.hpo_id = :test_hpo_id)
"""

# Metrics SQL Aggregations. 4-tuples of:
# - (MetricsKey) key: aggregation key
# - (str) sql: statement to select value, count for a metric (in that order)
# - (func(str): str) valuef: optional function which takes the value from the
#   above SQL output and converts it for presentation
# - (dict) params: optional extra SQL parameters to bind
_SqlAggregation = collections.namedtuple(
    '_SqlAggregation', ['key', 'sql', 'valuef', 'params'])


# Note that we depend on the participant_summary table containing only consented
# participants, by definition. Therefore these metrics only cover consented
# individuals.
_SQL_AGGREGATIONS = [
  _SqlAggregation(
      MetricsKey.ENROLLMENT_STATUS,
      """
      SELECT enrollment_status, COUNT(*)
      FROM participant_summary
      WHERE {summary_filter_sql}
      GROUP BY 1;
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      # Rewrite INTERESTED to CONSENTED, see note above.
      lambda v: ('CONSENTED' if v is EnrollmentStatus.INTERESTED.number
                 else EnrollmentStatus.lookup_by_number(v).name),
      None),
  # TODO(calbach): Verify whether we need to be conditionally trimming these
  # prefixes or leaving them unmodified. Unclear if all codes will have prefix
  # "PMI_".
  _SqlAggregation(
      MetricsKey.GENDER,
      """
      SELECT
        CASE
         WHEN code.value IS NULL THEN 'UNSET'
         ELSE code.value
        END, ps.count
      FROM (
       SELECT gender_identity_id, COUNT(*) count
       FROM participant_summary
       WHERE {summary_filter_sql}
       GROUP BY 1
      ) ps LEFT JOIN code
      ON ps.gender_identity_id = code.code_id;
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      None, None),
  _SqlAggregation(
      MetricsKey.RACE,
      """
      SELECT
        CASE
         WHEN race IS NULL THEN 0
         ELSE race
        END, COUNT(*)
      FROM participant_summary
      WHERE {summary_filter_sql}
      GROUP BY 1
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      lambda v: Race.lookup_by_number(v).name, None),
  _SqlAggregation(
      MetricsKey.STATE,
      """
      SELECT
        CASE
         WHEN code.value IS NULL THEN 'UNSET'
         WHEN code.value LIKE 'PIIState_%' THEN SUBSTR(code.value, LENGTH('PIIState_')+1)
         ELSE code.value
        END,
        ps.count
      FROM (
       SELECT state_id, COUNT(*) count
       FROM participant_summary
       WHERE {summary_filter_sql}
       GROUP BY 1
      ) ps LEFT JOIN code
      ON ps.state_id = code.code_id;
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      None, None),
  _SqlAggregation(
      MetricsKey.AGE_RANGE,
      """
      SELECT
        CASE
         WHEN date_of_birth IS NULL THEN 'UNSET'
         WHEN YEARS_OLD[:now, date_of_birth] < 0 THEN 'UNSET'
         WHEN YEARS_OLD[:now, date_of_birth] <= 17 THEN '0-17'
         WHEN YEARS_OLD[:now, date_of_birth] <= 25 THEN '18-25'
         WHEN YEARS_OLD[:now, date_of_birth] <= 35 THEN '26-35'
         WHEN YEARS_OLD[:now, date_of_birth] <= 45 THEN '36-45'
         WHEN YEARS_OLD[:now, date_of_birth] <= 55 THEN '46-55'
         WHEN YEARS_OLD[:now, date_of_birth] <= 65 THEN '56-65'
         WHEN YEARS_OLD[:now, date_of_birth] <= 75 THEN '66-75'
         WHEN YEARS_OLD[:now, date_of_birth] <= 85 THEN '76-85'
         ELSE '86+'
        END age_range,
        COUNT(*)
      FROM participant_summary
      WHERE {summary_filter_sql}
      GROUP BY 1;
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      None, None),
  _SqlAggregation(
      MetricsKey.PHYSICAL_MEASUREMENTS,
      """
      SELECT physical_measurements_status, COUNT(*)
      FROM participant_summary
      WHERE {summary_filter_sql}
      GROUP BY 1;
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      lambda v: PhysicalMeasurementsStatus.lookup_by_number(v).name,
      None),
  _SqlAggregation(
      MetricsKey.BIOSPECIMEN_SAMPLES,
      """
      SELECT
        CASE
         WHEN biospecimen_status IS NULL THEN 'UNSET'
         WHEN biospecimen_status = :unset_status THEN 'UNSET'
         WHEN biospecimen_status = :created_status THEN 'UNSET'
         ELSE 'COLLECTED'
        END, COUNT(*)
      FROM participant_summary
      WHERE {summary_filter_sql}
      GROUP BY 1;
      """.format(summary_filter_sql=_SUMMARY_FILTER_SQL),
      None,
      params={
        'unset_status': OrderStatus.UNSET.number,
        'created_status': OrderStatus.CREATED.number
      }),
  # TODO(calbach): Add healthcare_access, medical_history, medications,
  # family_health once available.
  _questionnaire_metric(MetricsKey.QUESTIONNAIRE_ON_OVERALL_HEALTH,
                        'questionnaire_on_overall_health'),
  # Personal habits is a newer naming for lifestyle
  _questionnaire_metric(MetricsKey.QUESTIONNAIRE_ON_PERSONAL_HABITS,
                        'questionnaire_on_lifestyle'),
  # Sociodemographics is a newer naming for 'the basics'
  _questionnaire_metric(MetricsKey.QUESTIONNAIRE_ON_SOCIODEMOGRAPHICS,
                        'questionnaire_on_the_basics'),
]


class PublicMetricsExport(object):
  """Exports data from the database needed to generate public registration metrics."""

  @staticmethod
  def export(metric_set_id):
    return PublicMetricsExport._save(metric_set_id, PublicMetricsExport._compute())

  @staticmethod
  def _compute():
    out = {}
    # Using a session here should put all following SQL invocations into a
    # non-locking read transaction per
    # https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
    now = clock.CLOCK.now()
    test_hpo = HPODao().get_by_name(TEST_HPO_NAME)
    with database_factory.make_server_cursor_database().session() as session:
      for (key, sql, valuef, params) in _SQL_AGGREGATIONS:
        sql = replace_years_old(sql)
        out[key] = []
        p = {
          'now': now,
          'test_hpo_id': test_hpo.hpoId,
          'test_email_pattern': TEST_EMAIL_PATTERN,
          'not_withdrawn_status': WithdrawalStatus.NOT_WITHDRAWN.number
        }
        if params:
          p.update(params)
        result = session.execute(text(sql), params=p)
        for row in result:
          v = row.items()[0][1]
          if valuef:
            v = valuef(v)
          out[key].append({
              'value': v,
              'count': row.items()[1][1],
          })
    return out

  @staticmethod
  def _save(metric_set_id, metrics):
    ms = MetricSet(
        metricSetId=metric_set_id,
        metricSetType=MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS,
        lastModified=clock.CLOCK.now()
    )
    db = database_factory.get_generic_database()
    def save(session):
      aggs = []
      MetricSetDao().upsert_with_session(session, ms)
      agg_dao = AggregateMetricsDao()
      agg_dao.delete_all_for_metric_set_with_session(session, metric_set_id)
      for (k, vals) in metrics.iteritems():
        for v in vals:
          agg = AggregateMetrics(
              metricSetId=metric_set_id,
              metricsKey=k,
              value=v['value'],
              count=v['count']
          )
          agg_dao.insert_with_session(session, agg)
          aggs.append(agg)
      return aggs
    return db.autoretry(save)
