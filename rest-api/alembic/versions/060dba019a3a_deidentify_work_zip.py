"""deidentify work zip

Revision ID: 060dba019a3a
Revises: 04564bf1b5e8
Create Date: 2018-01-04 09:51:00.885405

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from participant_enums import MetricSetType, MetricsKey
from model.site_enums import SiteStatus
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = '060dba019a3a'
down_revision = '04564bf1b5e8'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL = """
CREATE OR REPLACE VIEW questionnaire_response_answer_view AS
 SELECT
   p.participant_id participant_id,
   YEAR(p.sign_up_time) participant_sign_up_year,
   p.withdrawal_status participant_withdrawal_status,
   YEAR(p.withdrawal_time) participant_withdrawal_year,
   p.suspension_status participant_suspension_status,
   YEAR(p.suspension_time) participant_suspension_year,
   hpo.name hpo,
   (SELECT GROUP_CONCAT(concept_code.value SEPARATOR ',') FROM questionnaire_concept qc
     INNER JOIN code concept_code ON concept_code.code_id = qc.code_id
     WHERE qc.questionnaire_id = q.questionnaire_id) module,
   q.questionnaire_id questionnaire_id,
   q.created questionnaire_creation_time,
   qr.questionnaire_response_id questionnaire_response_id,
   YEAR(qr.created) questionnaire_response_submission_year,
   qc.value question_code,
   qc.code_id question_code_id,
   YEAR(qra.end_time) answer_end_year,
   ac.value answer_code,
   ac.code_id answer_code_id,
   qra.value_boolean answer_boolean,
   qra.value_decimal answer_decimal,
   IF(qc.value = 'EmploymentWorkAddress_ZipCode',
      NULL,
      qra.value_integer) answer_integer,
   IF(qra.value_string IS NULL, 0, 1) answer_string_present,
   YEAR(qra.value_date) answer_date_year,
   YEAR(qra.value_datetime) answer_datetime_year,
   IF(qra.value_uri IS NULL, 0, 1) answer_uri_present,
   IF(qc.value != 'EmploymentWorkAddress_ZipCode', NULL,
     IF(SUBSTR(LPAD(CONCAT(ps.zip_code), 5, '0'), 1, 3) IN (
          '036', '692', '878', '059', '790', '879', '063', '821', '884', '102',
          '823', '890', '203', '830', '893', '556', '831'),
        '000', SUBSTR(LPAD(CONCAT(ps.zip_code), 5, '0'), 1, 3)
     )) deidentified_zip_code
 FROM
   participant p
    INNER JOIN questionnaire_response qr ON p.participant_id = qr.participant_id
    INNER JOIN questionnaire_response_answer qra
       ON qra.questionnaire_response_id = qr.questionnaire_response_id
    INNER JOIN questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id
    INNER JOIN questionnaire q ON qq.questionnaire_id = q.questionnaire_id
    INNER JOIN code qc ON qq.code_id = qc.code_id
    INNER JOIN participant_summary ps ON p.participant_id = ps.participant_id
    LEFT OUTER JOIN hpo ON p.hpo_id = hpo.hpo_id
    LEFT OUTER JOIN code ac ON qra.value_code_id = ac.code_id
    WHERE (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
           (hpo.name IS NULL OR hpo.name != 'TEST')
"""


def upgrade_rdr():
  op.execute(_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL)


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
