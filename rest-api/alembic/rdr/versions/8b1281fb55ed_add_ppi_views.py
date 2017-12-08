"""Add PPI views

Revision ID: 8b1281fb55ed
Revises: 8d12872e0b77
Create Date: 2017-09-20 15:39:17.562293

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '8b1281fb55ed'
down_revision = '8d12872e0b77'
branch_labels = None
depends_on = None

# Creates a view that can be used by the Vanderbilt team (either directly or via CSV export)
# to analyze PPI quality.
_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL = """
CREATE VIEW questionnaire_response_answer_view AS
 SELECT
   p.participant_id participant_id,
   p.sign_up_time participant_sign_up_time,
   p.withdrawal_status participant_withdrawal_status,
   p.withdrawal_time participant_withdrawal_time,
   p.suspension_status participant_suspension_status,
   p.suspension_time participant_suspension_time,
   hpo.name hpo,
   (SELECT GROUP_CONCAT(concept_code.value SEPARATOR ',') FROM questionnaire_concept qc
     INNER JOIN code concept_code ON concept_code.code_id = qc.code_id
     WHERE qc.questionnaire_id = q.questionnaire_id) module,
   q.questionnaire_id questionnaire_id,
   q.created questionnaire_creation_time,
   qr.created questionnaire_response_submission_time,
   qc.value question_code,
   qc.code_id question_code_id,
   qra.end_time answer_end_time,
   ac.value answer_code,
   ac.code_id answer_code_id,
   qra.value_boolean answer_boolean,
   qra.value_decimal answer_decimal,
   qra.value_integer answer_integer,
   qra.value_string answer_string,
   qra.value_date answer_date,
   qra.value_datetime answer_datetime,
   qra.value_uri answer_uri
 FROM
   participant p
    INNER JOIN questionnaire_response qr ON p.participant_id = qr.participant_id
    INNER JOIN questionnaire_response_answer qra
       ON qra.questionnaire_response_id = qr.questionnaire_response_id
    INNER JOIN questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id
    INNER JOIN questionnaire q ON qq.questionnaire_id = q.questionnaire_id
    INNER JOIN code qc ON qq.code_id = qc.code_id
    INNER JOIN participant_summary ps
    LEFT OUTER JOIN hpo ON p.hpo_id = hpo.hpo_id
    LEFT OUTER JOIN code ac ON qra.value_code_id = ac.code_id
    WHERE (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
           (hpo.name IS NULL OR hpo.name != 'TEST')
"""
def upgrade():
  op.execute(_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL)

def downgrade():
  pass

