"""remote_id_fieleds

Revision ID: 7d92a6f2ff9b
Revises: e4dc63d75b9b
Create Date: 2022-12-29 10:00:35.212417

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
revision = '7d92a6f2ff9b'
down_revision = 'e4dc63d75b9b'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary',
                  sa.Column('remote_id_verification_origin', sa.String(length=80), nullable=True))
    op.add_column('participant_summary', sa.Column('remote_id_verification_status',
                                                   sa.Boolean(),
                                                   nullable=True))
    op.add_column('participant_summary', sa.Column('remote_id_verified_on',
                                                   sa.Date(),
                                                   nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'remote_id_verified_on')
    op.drop_column('participant_summary', 'remote_id_verification_status')
    op.drop_column('participant_summary', 'remote_id_verification_origin')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
