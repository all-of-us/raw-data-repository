from sqlalchemy import text
from dao import database_factory
from dao.database_utils import replace_years_old
from participant_enums import EnrollmentStatus, OrderStatus, PhysicalMeasurementsStatus
from participant_enums import Race, QuestionnaireStatus
from participant_enums import TEST_EMAIL_PATTERN, TEST_HPO_NAME


def _questionnaire_metric(name, col):
  """Returns a metrics SQL aggregation tuple for the given key/column."""
  return (
      name,
      """
      SELECT {col}, COUNT(*)
      FROM participant_summary
      WHERE {filter_test_sql}
      GROUP BY 1;
      """.format(col=col, filter_test_sql=_FILTER_TEST_SQL),
      lambda v: QuestionnaireStatus.lookup_by_number(v).name
  )

_FILTER_TEST_SQL = """
(NOT participant_summary.email LIKE '{test_email}'
 AND NOT participant_summary.hpo_id = '{test_hpo}')
""".format(test_email=TEST_EMAIL_PATTERN, test_hpo=TEST_HPO_NAME)

# Metrics definitions. 3-tuples of:
# - (str) aggregation key name
# - (str) SQL statement to select value, count for a metric (in that order)
# - (func(str): str) optional function which takes the value from the above SQL
#   output and converts it for presentation
#
# Note that we depend on the participant_summary table containing only consented
# participants, by definition. Therefore these metrics only cover consented
# individuals.
_SQL_AGGREGATIONS = [
  ('enrollmentStatus',
   """
   SELECT enrollment_status, COUNT(*)
   FROM participant_summary
   WHERE {filter_test_sql}
   GROUP BY 1;
   """.format(filter_test_sql=_FILTER_TEST_SQL),
   lambda v: EnrollmentStatus.lookup_by_number(v).name),
  # TODO(calbach): Verify whether we need to be conditionally trimming these
  # prefixes or leaving them unmodified. Unclear if all codes will have prefix
  # "PMI_".
  ('gender',
   """
   SELECT
     CASE
      WHEN code.value IS NULL THEN 'UNSET'
      ELSE code.value
     END, ps.count
   FROM (
    SELECT gender_identity_id, COUNT(*) count
    FROM participant_summary
    WHERE {filter_test_sql}
    GROUP BY 1
   ) ps LEFT JOIN code
   ON ps.gender_identity_id = code.code_id;
   """.format(filter_test_sql=_FILTER_TEST_SQL),
   None),
  ('race',
   """
   SELECT
     CASE
      WHEN race IS NULL THEN 0
      ELSE race
     END, COUNT(*)
   FROM participant_summary
   WHERE {filter_test_sql}
   GROUP BY 1
   """.format(filter_test_sql=_FILTER_TEST_SQL),
   lambda v: Race.lookup_by_number(v).name),
  ('state',
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
    WHERE {filter_test_sql}
    GROUP BY 1
   ) ps LEFT JOIN code
   ON ps.state_id = code.code_id;
   """.format(filter_test_sql=_FILTER_TEST_SQL),
   None),
  ('ageRange',
   """
   SELECT
     CASE
      WHEN date_of_birth IS NULL THEN 'UNSET'
      WHEN YEARS_OLD[date_of_birth] < 0 THEN 'UNSET'
      WHEN YEARS_OLD[date_of_birth] <= 17 THEN '0-17'
      WHEN YEARS_OLD[date_of_birth] <= 25 THEN '18-25'
      WHEN YEARS_OLD[date_of_birth] <= 35 THEN '26-35'
      WHEN YEARS_OLD[date_of_birth] <= 45 THEN '36-45'
      WHEN YEARS_OLD[date_of_birth] <= 55 THEN '46-55'
      WHEN YEARS_OLD[date_of_birth] <= 65 THEN '56-65'
      WHEN YEARS_OLD[date_of_birth] <= 75 THEN '66-75'
      WHEN YEARS_OLD[date_of_birth] <= 85 THEN '76-85'
      ELSE '86+'
     END age_range,
     COUNT(*)
   FROM participant_summary
   WHERE {filter_test_sql}
   GROUP BY 1;
   """.format(filter_test_sql=_FILTER_TEST_SQL),
   None),
  ('physicalMeasurements',
   """
   SELECT physical_measurements_status, COUNT(*)
   FROM participant_summary
   WHERE {filter_test_sql}
   GROUP BY 1;
   """.format(filter_test_sql=_FILTER_TEST_SQL),
   lambda v: PhysicalMeasurementsStatus.lookup_by_number(v).name),
  ('biospecimenSamples',
   """
   SELECT
     CASE
      WHEN biospecimen_status IS NULL THEN 'UNSET'
      WHEN biospecimen_status = '{unset}' THEN 'UNSET'
      WHEN biospecimen_status = '{created}' THEN 'UNSET'
      ELSE 'COLLECTED'
     END, COUNT(*)
   FROM participant_summary
   WHERE {filter_test_sql}
   GROUP BY 1;
   """.format(unset=str(OrderStatus.UNSET.number),
              created=str(OrderStatus.CREATED.number),
              filter_test_sql=_FILTER_TEST_SQL),
   None),
  # TODO(calbach): Add healthcare_access, medical_history, medications,
  # family_health once available.
  _questionnaire_metric('questionnaireOnOverallHealth', 'questionnaire_on_overall_health'),
  # Personal habits is a newer naming for lifestyle
  _questionnaire_metric('questionnaireOnPersonalHabits', 'questionnaire_on_lifestyle'),
  # Sociodemographics is a newer naming for 'the basics'
  _questionnaire_metric('questionnaireOnSociodemographics', 'questionnaire_on_the_basics'),
]


class PublicMetricsExport(object):
  """Exports data from the database needed to generate public registration metrics."""

  @staticmethod
  def export():
    # TODO(calbach): Write the output to a given target destination rather than
    # returning it.
    out = {}
    # Using a session here should put all following SQL invocations into a
    # non-locking read transaction per
    # https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
    with database_factory.make_server_cursor_database().session() as session:
      for (key, sql, valuef) in _SQL_AGGREGATIONS:
        sql = replace_years_old(sql)
        out[key] = []
        result = session.execute(text(sql))
        for row in result:
          v = row.items()[0][1]
          if valuef:
            v = valuef(v)
          out[key].append({
              'value': v,
              'count': row.items()[1][1],
          })
    return out
