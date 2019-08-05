"""remove history constraints

Revision ID: 03bdb0a44083
Revises: 534d805d5dcf
Create Date: 2019-03-21 15:32:02.572675

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus
from rdr_service.dao.alembic_utils import ReplaceableObject
# revision identifiers, used by Alembic.
revision = '03bdb0a44083'
down_revision = '534d805d5dcf'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()




sp_drop_index_if_exists = ReplaceableObject('sp_drop_index_if_exists',
"""
  (
    IN in_table VARCHAR(255),
    IN in_index VARCHAR(255)
  )
  BEGIN
    # Drop the index only if it exists.
    IF ((SELECT COUNT(*)
          FROM information_schema.statistics
          WHERE table_schema = DATABASE() AND
                table_name = in_table AND
                index_name = in_index) > 0) THEN
      SET @sql = CONCAT('DROP INDEX ', in_index, ' ON ', in_table);
      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

    END IF;
  END
""")


def upgrade(engine_name):
  globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
  globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
  op.create_sp(sp_drop_index_if_exists)

  op.execute("""call sp_drop_index_if_exists('biobank_dv_order_history', 'uidx_partic_id_order_id')""")
  op.execute("""call sp_drop_index_if_exists('biobank_dv_order_history', 'biobank_order_id')""")
  op.execute("""call sp_drop_index_if_exists('biobank_dv_order_history', 'biobank_state_id')""")
  op.execute("""call sp_drop_index_if_exists('biobank_dv_order_history', 'state_id')""")


def downgrade_rdr():
  op.drop_sp(sp_drop_index_if_exists)

def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
