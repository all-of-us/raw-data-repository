"""adding new sample types to participant summary

Revision ID: 857abc9469aa
Revises: bd97e2a3b66b
Create Date: 2023-10-10 13:34:29.270917

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
revision = '857abc9469aa'
down_revision = 'bd97e2a3b66b'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    op.execute("""
        alter table participant_summary
        add column (
            sample_order_status_2ed02 smallint DEFAULT NULL,
            sample_order_status_2ed02_time datetime DEFAULT NULL,
            sample_order_status_2ed04 smallint DEFAULT NULL,
            sample_order_status_2ed04_time datetime DEFAULT NULL,
            sample_status_2ed02 smallint DEFAULT NULL,
            sample_status_2ed02_time datetime DEFAULT NULL,
            sample_status_2ed04 smallint DEFAULT NULL,
            sample_status_2ed04_time datetime DEFAULT NULL
        );
    """)


def downgrade_rdr():
    op.drop_column('participant_summary', 'sample_status_2ed04_time')
    op.drop_column('participant_summary', 'sample_status_2ed04')
    op.drop_column('participant_summary', 'sample_status_2ed02_time')
    op.drop_column('participant_summary', 'sample_status_2ed02')
    op.drop_column('participant_summary', 'sample_order_status_2ed04_time')
    op.drop_column('participant_summary', 'sample_order_status_2ed04')
    op.drop_column('participant_summary', 'sample_order_status_2ed02_time')
    op.drop_column('participant_summary', 'sample_order_status_2ed02')


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

