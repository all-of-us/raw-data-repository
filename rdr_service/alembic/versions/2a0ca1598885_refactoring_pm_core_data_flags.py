"""refactoring pm core data flags

Revision ID: 2a0ca1598885
Revises: 3d95302cfb96
Create Date: 2023-06-28 09:56:48.918290

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
revision = '2a0ca1598885'
down_revision = '3d95302cfb96'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    op.execute("""
        alter table physical_measurements
        add column (
            meets_height_reqs BOOL,
            meets_weight_reqs BOOL
        );
    """)


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('physical_measurements', 'meets_weight_reqs')
    op.drop_column('physical_measurements', 'meets_height_reqs')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

