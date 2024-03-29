"""computed_health_data_sharing_cols_update

Revision ID: 6747d878e5d1
Revises: c199139b10c2
Create Date: 2022-12-09 02:15:40.143525

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
revision = '6747d878e5d1'
down_revision = 'c199139b10c2'
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
        MODIFY COLUMN health_data_stream_sharing_status_v_3_1 smallint(6) GENERATED ALWAYS AS (
          (case when is_ehr_data_available then 3
                when was_ehr_data_available then 2
                when was_participant_mediated_ehr_available then 2
                else 1 end)) STORED,
        MODIFY COLUMN health_data_stream_sharing_status_v_3_1_time datetime GENERATED ALWAYS AS (
            NULLIF(GREATEST(COALESCE(ehr_update_time, 0), COALESCE(latest_participant_mediated_ehr_receipt_time, 0)), 0)
        ) STORED
    """)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("""
        ALTER TABLE participant_summary
        MODIFY COLUMN health_data_stream_sharing_status_v_3_1 smallint(6) GENERATED ALWAYS AS (
          (case when is_ehr_data_available then 3 when was_ehr_data_available then 2 else 1 end)) STORED,
        MODIFY COLUMN health_data_stream_sharing_status_v_3_1_time datetime GENERATED ALWAYS AS (ehr_update_time) STORED
    """)

    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

