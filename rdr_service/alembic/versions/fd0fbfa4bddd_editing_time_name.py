"""editing_time_name

Revision ID: fd0fbfa4bddd
Revises: f0d635507938, 2ddb58c2b603, ace982c2cb2b, 3bd1cf3a498d
Create Date: 2022-09-15 09:43:16.784537

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
revision = 'fd0fbfa4bddd'
down_revision = ('f0d635507938', '2ddb58c2b603', 'ace982c2cb2b', '3bd1cf3a498d')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_appointment_event', sa.Column('appointment_timestamp', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True))
    op.drop_column('genomic_appointment_event', 'appointment_time')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_appointment_event', sa.Column('appointment_time', mysql.DATETIME(fsp=6), nullable=True))
    op.drop_column('genomic_appointment_event', 'appointment_timestamp')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

