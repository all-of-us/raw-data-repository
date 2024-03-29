"""participant_mediated_ehr

Revision ID: c199139b10c2
Revises: bd7e69e0e71c, 7338c2929edc, d1472478ce8a
Create Date: 2022-12-06 08:55:48.026974

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
revision = 'c199139b10c2'
down_revision = ('bd7e69e0e71c', '7338c2929edc', 'd1472478ce8a')
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
                  ADD COLUMN was_participant_mediated_ehr_available boolean not null default 0,
                  ADD COLUMN first_participant_mediated_ehr_receipt_time datetime null default null,
                  ADD COLUMN latest_participant_mediated_ehr_receipt_time datetime null default null;
    """)

    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("""ALTER TABLE participant_summary
                  DROP COLUMN was_participant_mediated_ehr_available,
                  DROP COLUMN first_participant_mediated_ehr_receipt_time,
                  DROP COLUMN latest_participant_mediated_ehr_receipt_time;
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

