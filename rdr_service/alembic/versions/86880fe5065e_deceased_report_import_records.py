"""deceased report import records

Revision ID: 86880fe5065e
Revises: 8cc481fa7390
Create Date: 2020-10-27 11:51:59.814691

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
revision = '86880fe5065e'
down_revision = '8cc481fa7390'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('deceased_report_import_record',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('last_seen', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('deceased_report_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['deceased_report_id'], ['deceased_report.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('deceased_report_import_record')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
