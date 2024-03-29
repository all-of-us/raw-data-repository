"""adding duplicate account table

Revision ID: 2a0cc5df5d49
Revises: f254bca8e484
Create Date: 2024-02-29 10:54:52.673161

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
revision = '2a0cc5df5d49'
down_revision = 'f254bca8e484'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('duplicate_accounts',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('participant_a_id', sa.Integer(), nullable=False),
    sa.Column('participant_b_id', sa.Integer(), nullable=False),
    sa.Column('primary_participant', sa.Enum('PARTICIPANT_A', 'PARTICIPANT_B', name='primaryparticipantindication'), nullable=True),
    sa.Column('authored', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('status', sa.Enum('POTENTIAL', 'APPROVED', 'REJECTED', name='duplicationstatus'), nullable=False),
    sa.Column('source', sa.Enum('RDR', 'SUPPORT_TICKET', 'VIBRENT', name='duplicationsource'), nullable=False),
    sa.ForeignKeyConstraint(['participant_a_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['participant_b_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_duplicate_accounts_participant_a_id'), 'duplicate_accounts', ['participant_a_id'], unique=False)
    op.create_index(op.f('ix_duplicate_accounts_participant_b_id'), 'duplicate_accounts', ['participant_b_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_duplicate_accounts_participant_b_id'), table_name='duplicate_accounts')
    op.drop_index(op.f('ix_duplicate_accounts_participant_a_id'), table_name='duplicate_accounts')
    op.drop_table('duplicate_accounts')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

