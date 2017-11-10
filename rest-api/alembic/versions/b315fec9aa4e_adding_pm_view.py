"""Adding pm view

Revision ID: b315fec9aa4e
Revises: 9a5c2ef1038f
Create Date: 2017-11-10 10:07:29.266546

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b315fec9aa4e'
down_revision = '9a5c2ef1038f'
branch_labels = None
depends_on = None


# Creates a view that can be used by the Vanderbilt team (either directly or via CSV export)
# to analyze finalized physical measurements.
_MEASUREMENTS_VIEW_SQL = """
CREATE VIEW physical_measurements_view AS
 SELECT
   p.participant_id participant_id,
   p.sign_up_time participant_sign_up_time,
   p.withdrawal_status participant_withdrawal_status,
   p.withdrawal_time participant_withdrawal_time,
   p.suspension_status participant_suspension_status,
   p.suspension_time participant_suspension_time,
   pm.physical_measurements_id physical_measurements_id,   
   pm.created created,
   pm.amended_measurements_id amended_measurements_id,
   pm.created_site_id created_site_id,
   pm.created_username created_username,
   pm.finalized_site_id finalized_site_id,
   pm.finalized_username finalized_username,   
   m.measurement_id measurement_id,
   m.code_system code_system,
   m.code_value code_value,
   m.measurement_time measurement_time,
   m.body_site_code_system body_site_code_system,
   m.body_site_code_value body_site_code_value,
   m.value_string value_string,
   m.value_decimal value_decimal,
   m.value_unit value_unit,
   m.value_code_system value_code_system,
   m.value_code_value value_code_value,
   m.value_datetime value_datetime,
   m.parent_id parent_id,
   m.qualifier_id qualifier_id
 FROM 
   participant p
    INNER JOIN physical_measurements pm
       ON pm.participant_id = p.participant_id
    INNER JOIN measurement m
       ON pm.physical_measurements_id = m.physical_measurements_id
    INNER JOIN participant_summary ps 
       ON p.participant_id = ps.participant_id
    LEFT OUTER JOIN hpo 
       ON p.hpo_id = hpo.hpo_id
  WHERE (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
        (hpo.name IS NULL OR hpo.name != 'TEST') AND
        pm.final = 1 
"""
def upgrade():
  op.execute(_MEASUREMENTS_VIEW_SQL)

def downgrade():
  pass
