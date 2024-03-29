"""unique constraint on participant file times

Revision ID: ba05cd337cdc
Revises: c57da0c2679e
Create Date: 2020-11-17 09:24:55.670171

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
revision = 'ba05cd337cdc'
down_revision = 'c57da0c2679e'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    op.drop_constraint("participant_ehr_receipt_ibfk_1", "participant_ehr_receipt", type_="foreignkey")

    op.drop_index('idx_participant_ehr_receipt_participant_file_time', table_name='participant_ehr_receipt')
    op.create_index('idx_participant_ehr_receipt_participant_file_time', 'participant_ehr_receipt', ['participant_id', 'file_timestamp'], unique=True)

    op.create_foreign_key(
        None, "participant_ehr_receipt", "participant", ["participant_id"], ["participant_id"], ondelete="CASCADE"
    )


def downgrade_rdr():
    op.drop_constraint("participant_ehr_receipt_ibfk_1", "participant_ehr_receipt", type_="foreignkey")

    op.drop_index('idx_participant_ehr_receipt_participant_file_time', table_name='participant_ehr_receipt')
    op.create_index('idx_participant_ehr_receipt_participant_file_time', 'participant_ehr_receipt', ['participant_id', 'file_timestamp'], unique=False)

    op.create_foreign_key(
        None, "participant_ehr_receipt", "participant", ["participant_id"], ["participant_id"], ondelete="CASCADE"
    )


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
