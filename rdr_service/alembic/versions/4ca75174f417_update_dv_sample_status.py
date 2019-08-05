"""update dv sample status

Revision ID: 4ca75174f417
Revises: 6057344ac521
Create Date: 2019-03-15 10:16:48.077068

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from sqlalchemy.dialects import mysql

from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = '4ca75174f417'
down_revision = '6057344ac521'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('biobank_dv_order', 'created',
               existing_type=mysql.DATETIME(),
               nullable=True)
    op.alter_column('biobank_dv_order', 'modified',
               existing_type=mysql.DATETIME(fsp=6),
               nullable=True,
               existing_server_default=sa.text(u'current_timestamp(6) ON UPDATE current_timestamp(6)'))
    op.create_index(op.f('ix_biobank_stored_sample_test'), 'biobank_stored_sample', ['test'], unique=False)
    op.execute(
      'ALTER TABLE biobank_dv_order CHANGE COLUMN `created` `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) AFTER id;')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_biobank_stored_sample_test'), table_name='biobank_stored_sample')
    op.alter_column('biobank_dv_order', 'modified',
               existing_type=mysql.DATETIME(fsp=6),
               nullable=False,
               existing_server_default=sa.text(u'current_timestamp(6) ON UPDATE current_timestamp(6)'))
    op.alter_column('biobank_dv_order', 'created',
               existing_type=mysql.DATETIME(),
               nullable=False)
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

