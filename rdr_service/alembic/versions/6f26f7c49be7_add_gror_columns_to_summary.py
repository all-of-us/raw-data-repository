"""add gror columns to summary

Revision ID: 6f26f7c49be7
Revises: 64e68e221460
Create Date: 2020-02-17 14:42:24.710777

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = '6f26f7c49be7'
down_revision = '64e68e221460'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('consent_for_genomics_ror', model.utils.Enum(QuestionnaireStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('consent_for_genomics_ror_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('consent_for_genomics_ror_authored', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('semantic_version_for_primary_consent', sa.String(length=100), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'semantic_version_for_primary_consent')
    op.drop_column('participant_summary', 'consent_for_genomics_ror_time')
    op.drop_column('participant_summary', 'consent_for_genomics_ror_authored')
    op.drop_column('participant_summary', 'consent_for_genomics_ror')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

