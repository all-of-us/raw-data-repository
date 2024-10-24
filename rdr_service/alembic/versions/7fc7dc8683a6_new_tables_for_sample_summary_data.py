"""new tables for sample summary data

Revision ID: 7fc7dc8683a6
Revises: c2af50164c3e
Create Date: 2024-05-15 09:33:58.908011

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = '7fc7dc8683a6'
down_revision = 'c2af50164c3e'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('sample_order_status',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('test_code', sa.String(length=80), nullable=False),
    sa.Column('status', rdr_service.model.utils.Enum(OrderStatus), nullable=False),
    sa.Column('status_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('sample_receipt_status',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('test_code', sa.String(length=80), nullable=False),
    sa.Column('status', rdr_service.model.utils.Enum(SampleStatus), nullable=False),
    sa.Column('status_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade_rdr():
    op.drop_table('sample_receipt_status')
    op.drop_table('sample_order_status')


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

