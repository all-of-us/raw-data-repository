from sqlalchemy import text
from dao import database_factory
from dao.database_utils import replace_years_old
from participant_enums import EnrollmentStatus, OrderStatus, PhysicalMeasurementsStatus
from participant_enums import Race, QuestionnaireStatus

# TODO(calbach): consider whether we need to filter down to consented
# individuals only.

def _questionnaire_metric(name, col):
  """Returns a metrics SQL aggregation tuple for the given key/column."""
  return (
      name,
      """
      SELECT {}, SUM(1)
      FROM participant_summary
      GROUP BY 1;
      """.format(col),
      lambda v: QuestionnaireStatus.lookup_by_number(v).name
  )

# Metrics definitions. 3-tuples of:
# - (str) aggregation key name
# - (str) SQL statement to select value, count for a metric (in that order)
# - (func(str): str) optional function which takes the value from the above SQL
#   output and converts it for presentation
_SQL_AGGREGATIONS = [
  ('enrollmentStatus',
   """
   SELECT enrollment_status, SUM(1)
   FROM participant_summary
   GROUP BY 1;
   """,
   lambda v: EnrollmentStatus.lookup_by_number(v).name),
  # TODO(calbach): Verify whether we need to be conditionally trimming these
  # prefixes or leaving them unmodified. Unclear if all codes will have prefix
  # "PMI_".
  ('gender',
   """
   SELECT code.value, ps.count
   FROM (
    SELECT gender_identity_id, SUM(1) count
    FROM participant_summary
    WHERE gender_identity_id IS NOT NULL
    GROUP BY 1
   ) ps LEFT JOIN code
   ON ps.gender_identity_id = code.code_id;
   """,
   None),
  ('race',
   """
   SELECT race, SUM(1)
   FROM participant_summary
   WHERE race IS NOT NULL
   GROUP BY 1
   """,
   lambda v: Race.lookup_by_number(v).name),
  ('state',
   """
   SELECT SUBSTR(code.value, {}), ps.count
   FROM (
    SELECT state_id, SUM(1) count
    FROM participant_summary
    WHERE state_id IS NOT NULL
    GROUP BY 1
   ) ps LEFT JOIN code
   ON ps.state_id = code.code_id;
   """.format(len('PIIState_') + 1),
   None),
  ('ageRange',
   """
   SELECT
     CASE
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
     SUM(1)
   FROM participant_summary
   GROUP BY 1;
   """,
   None),
  ('physicalMeasurements',
   """
   SELECT physical_measurements_status, SUM(1)
   FROM participant_summary
   GROUP BY 1;
   """,
   lambda v: PhysicalMeasurementsStatus.lookup_by_number(v).name),
  ('biospecimenSamples',
   """
   SELECT
     CASE (biospecimen_status)
      WHEN '{}' THEN 'UNSET'
      WHEN '{}' THEN 'UNSET'
      ELSE 'COLLECTED'
     END, SUM(1)
   FROM participant_summary
   GROUP BY 1;
   """.format(str(OrderStatus.UNSET.number), str(OrderStatus.CREATED.number)),
   None),
  _questionnaire_metric('questionnaireOnOverallHealth', 'questionnaire_on_overall_health'),
  # Personal habits is a newer naming for lifestyle
  _questionnaire_metric('questionnaireOnPersonalHabits', 'questionnaire_on_lifestyle'),
  # Sociodemographics is a newer naming for 'the basics'
  _questionnaire_metric('questionnaireOnSociodemographics', 'questionnaire_on_the_basics'),
  _questionnaire_metric('questionnaireOnHealthcareAccess', 'questionnaire_on_healthcare_access'),
  _questionnaire_metric('questionnaireOnMedicalHistory', 'questionnaire_on_medical_history'),
  _questionnaire_metric('questionnaireOnMedications', 'questionnaire_on_medications'),
  _questionnaire_metric('questionnaireOnFamilyHealth', 'questionnaire_on_family_health')
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
