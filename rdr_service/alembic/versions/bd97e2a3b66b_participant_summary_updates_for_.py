"""participant summary updates for retention and enrollment status

Revision ID: bd97e2a3b66b
Revises: b7d11fd04fdc
Create Date: 2023-09-23 10:49:13.722585

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
revision = 'bd97e2a3b66b'
down_revision = '46ef6c371e79'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    # Overriding auto generatoed individual op.add_column() operations with single ALTER TABLE operation, for efficiency
    op.execute("""
         alter table participant_summary
         add column (
             has_height_and_weight BOOL,
             has_height_and_weight_time DATETIME,
             consent_for_wear_study SMALLINT,
             consent_for_wear_study_time DATETIME,
             consent_for_wear_study_authored DATETIME,
             latest_etm_task_authored DATETIME,
             latest_etm_task_time DATETIME
         );
     """)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'latest_etm_task_time')
    op.drop_column('participant_summary', 'latest_etm_task_authored')
    op.drop_column('participant_summary', 'consent_for_wear_study_time')
    op.drop_column('participant_summary', 'consent_for_wear_study_authored')
    op.drop_column('participant_summary', 'consent_for_wear_study')
    op.drop_column('participant_summary', 'has_height_and_weight')
    op.drop_column('participant_summary', 'has_height_and_weight_time')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

