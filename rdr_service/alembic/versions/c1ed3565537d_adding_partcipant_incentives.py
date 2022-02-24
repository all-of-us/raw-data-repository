"""adding_partcipant_incentives

Revision ID: c1ed3565537d
Revises: bd90a19851f8, 24f6c59ecd4c
Create Date: 2022-02-16 10:09:40.742385

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = 'c1ed3565537d'
down_revision = ('bd90a19851f8', '24f6c59ecd4c')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('participant_incentives',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('modified', sa.DateTime(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('created_by', sa.String(length=255), nullable=False),
    sa.Column('site', sa.Integer(), nullable=False),
    sa.Column('date_given', sa.String(length=255), nullable=False),
    sa.Column('incentive_type', sa.String(length=255), nullable=False),
    sa.Column('giftcard_type', sa.String(length=255), nullable=True),
    sa.Column('amount', sa.SmallInteger(), nullable=False),
    sa.Column('occurrence', sa.String(length=255), nullable=False),
    sa.Column('notes', sa.String(length=512), nullable=True),
    sa.Column('cancelled', sa.SmallInteger(), nullable=False),
    sa.Column('cancelled_by', sa.String(length=255), nullable=True),
    sa.Column('cancelled_date', sa.String(length=255), nullable=True),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['site'], ['site.site_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_participant_incentives_participant_id'), 'participant_incentives', ['participant_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_participant_incentives_participant_id'), table_name='participant_incentives')
    op.drop_table('participant_incentives')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
