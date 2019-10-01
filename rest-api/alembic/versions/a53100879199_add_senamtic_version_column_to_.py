"""add senamtic_version column to questionnaire

Revision ID: a53100879199
Revises: 4a4457c6b497
Create Date: 2019-10-01 10:13:58.703085

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from sqlalchemy.dialects import mysql

from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from participant_enums import MetricSetType, MetricsKey, GenderIdentity
from model.base import add_table_history_table, drop_table_history_table
from model.code import CodeType
from model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = 'a53100879199'
down_revision = '4a4457c6b497'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('questionnaire', sa.Column('semantic_version', sa.String(length=100), nullable=True))
    op.add_column('questionnaire_history', sa.Column('semantic_version', sa.String(length=100), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('questionnaire_history', 'semantic_version')
    op.drop_column('questionnaire', 'semantic_version')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

