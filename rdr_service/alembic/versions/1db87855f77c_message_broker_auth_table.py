"""message broker auth table

Revision ID: 1db87855f77c
Revises: 93fce807f225
Create Date: 2021-06-07 16:26:13.700032

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
revision = '1db87855f77c'
down_revision = '93fce807f225'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('Message_broker_dest_auth_info',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('destination', sa.String(length=80), nullable=True),
    sa.Column('key', sa.String(length=256), nullable=True),
    sa.Column('secret', sa.String(length=256), nullable=True),
    sa.Column('token_endpoint', sa.String(length=512), nullable=True),
    sa.Column('access_token', sa.String(length=512), nullable=True),
    sa.Column('expired_at', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('destination', name='unique_destination')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('Message_broker_dest_auth_info')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

