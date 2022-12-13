"""adding 3.0 and 3.1 enrollment status fields to summary

Revision ID: 47fdbc544e62
Revises: 11234f47671d
Create Date: 2022-09-06 14:20:08.859243

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
revision = '47fdbc544e62'
down_revision = '11234f47671d'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("""
        ALTER TABLE participant_summary
        ADD COLUMN enrollment_status_v_3_0 smallint(6),
        ADD COLUMN enrollment_status_participant_v_3_0_time DATETIME,
        ADD COLUMN enrollment_status_participant_plus_ehr_v_3_0_time DATETIME,
        ADD COLUMN enrollment_status_pmb_eligible_v_3_0_time DATETIME,
        ADD COLUMN enrollment_status_core_minus_pm_v_3_0_time DATETIME,
        ADD COLUMN enrollment_status_core_v_3_0_time DATETIME,
        ADD COLUMN enrollment_status_v_3_1 smallint(6),
        ADD COLUMN enrollment_status_participant_v_3_1_time DATETIME,
        ADD COLUMN enrollment_status_participant_plus_ehr_v_3_1_time DATETIME,
        ADD COLUMN enrollment_status_participant_plus_basics_v_3_1_time DATETIME,
        ADD COLUMN enrollment_status_core_minus_pm_v_3_1_time DATETIME,
        ADD COLUMN enrollment_status_core_v_3_1_time DATETIME,
        ADD COLUMN enrollment_status_participant_plus_baseline_v_3_1_time DATETIME
    """)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'enrollment_status_v_3_1')
    op.drop_column('participant_summary', 'enrollment_status_v_3_0')
    op.drop_column('participant_summary', 'enrollment_status_pmb_eligible_v_3_0_time')
    op.drop_column('participant_summary', 'enrollment_status_participant_v_3_1_time')
    op.drop_column('participant_summary', 'enrollment_status_participant_v_3_0_time')
    op.drop_column('participant_summary', 'enrollment_status_participant_plus_ehr_v_3_0_time')
    op.drop_column('participant_summary', 'enrollment_status_participant_plus_basics_v_3_1_time')
    op.drop_column('participant_summary', 'enrollment_status_participant_plus_baseline_v_3_1_time')
    op.drop_column('participant_summary', 'enrollment_status_participant_plusEhr_v_3_1_time')
    op.drop_column('participant_summary', 'enrollment_status_core_v_3_1_time')
    op.drop_column('participant_summary', 'enrollment_status_core_v_3_0_time')
    op.drop_column('participant_summary', 'enrollment_status_core_minus_pm_v_3_1_time')
    op.drop_column('participant_summary', 'enrollment_status_core_minus_pm_v_3_0_time')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
