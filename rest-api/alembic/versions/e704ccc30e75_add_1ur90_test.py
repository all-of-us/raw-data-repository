"""add 1UR90 test

Revision ID: e704ccc30e75
Revises: f098d2c51614
Create Date: 2018-04-11 08:25:34.932667

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from participant_enums import MetricSetType, MetricsKey
from model.site_enums import SiteStatus, EnrollingStatus
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = 'e704ccc30e75'
down_revision = 'f098d2c51614'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('sample_order_status_1ur90', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ur90_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_status_1ur90', model.utils.Enum(SampleStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_status_1ur90_time', model.utils.UTCDateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'sample_status_1ur90_time')
    op.drop_column('participant_summary', 'sample_status_1ur90')
    op.drop_column('participant_summary', 'sample_order_status_1ur90_time')
    op.drop_column('participant_summary', 'sample_order_status_1ur90')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

