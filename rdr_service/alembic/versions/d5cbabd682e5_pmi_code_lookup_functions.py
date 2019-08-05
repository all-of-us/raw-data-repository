"""PMI code lookup functions

Revision ID: d5cbabd682e5
Revises: 5aa0142f9bcb
Create Date: 2019-02-27 15:15:18.476588

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from sqlalchemy.dialects import mysql
from rdr_service.dao.alembic_utils import ReplaceableObject

from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus
from rdr_service.model.code import CodeType

# revision identifiers, used by Alembic.
revision = 'd5cbabd682e5'
down_revision = '014053444333'
branch_labels = None
depends_on = None


fn_get_code_module = ReplaceableObject('fn_get_code_module',
  """  
  (pmi_code VARCHAR(80))
  RETURNS VARCHAR(80)  
  BEGIN
    # Return the top most parent PMI code of the given PMI code, the code may be 
    # of any code_type (1 thru 4).  
    RETURN (
      SELECT COALESCE(c4.short_value, c3.short_value, c2.short_value, c1.short_value)
      FROM rdr.code c1
             LEFT JOIN rdr.code c2 on c1.parent_id = c2.code_id
             LEFT JOIN rdr.code c3 on c2.parent_id = c3.code_id
             LEFT JOIN rdr.code c4 on c3.parent_id = c4.code_id
      WHERE c1.short_value = pmi_code
         OR c1.value = pmi_code
    );
  
  END
  """)

fn_get_code_module_id = ReplaceableObject('fn_get_code_module_id',
  """
  (pmi_code VARCHAR(80))
  RETURNS INT    
  BEGIN
    # Return the top most parent ID of the given PMI code, the code may be 
    # of any code_type (1 thru 4).  
    RETURN (
      SELECT COALESCE(c4.code_id, c3.code_id, c2.code_id, c1.code_id)
      FROM rdr.code c1
             LEFT JOIN rdr.code c2 on c1.parent_id = c2.code_id
             LEFT JOIN rdr.code c3 on c2.parent_id = c3.code_id
             LEFT JOIN rdr.code c4 on c3.parent_id = c4.code_id
      WHERE c1.short_value = pmi_code
         OR c1.value = pmi_code
    );
  
  END
  """
)

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('participant', 'last_modified',
               existing_type=mysql.DATETIME(fsp=6),
               nullable=False)
    # ### end Alembic commands ###

    op.create_fn(fn_get_code_module)
    op.create_fn(fn_get_code_module_id)


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('participant', 'last_modified',
               existing_type=mysql.DATETIME(fsp=6),
               nullable=True)
    # ### end Alembic commands ###

    op.drop_fn(fn_get_code_module)
    op.drop_fn(fn_get_code_module_id)

def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def unittest_schemas():
  schemas = list()

  schemas.append('DROP FUNCTION IF EXISTS `{0}`'.format(fn_get_code_module.name))
  schemas.append('CREATE FUNCTION `{0}` {1}'.format(
                  fn_get_code_module.name, fn_get_code_module.sqltext))

  schemas.append('DROP FUNCTION IF EXISTS `{0}`'.format(fn_get_code_module_id.name))
  schemas.append('CREATE FUNCTION `{0}` {1}'.format(
                  fn_get_code_module_id.name, fn_get_code_module_id.sqltext))

  return schemas
