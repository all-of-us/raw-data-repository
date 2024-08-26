"""survey validation results table

Revision ID: 0e2d0a24dbea
Revises: 7fc7dc8683a6, c33b3da0905a
Create Date: 2024-08-26 08:32:22.947962

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
revision = '0e2d0a24dbea'
down_revision = ('7fc7dc8683a6', 'c33b3da0905a')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('ppi_validation_results',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('questionnaire_response_id', sa.Integer(), nullable=False),
    sa.Column('survey_id', sa.Integer(), nullable=False),
    sa.Column('obsoletion_timestamp', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('obsoletion_reason', sa.String(length=512), nullable=True),
    sa.ForeignKeyConstraint(['questionnaire_response_id'], ['questionnaire_response.questionnaire_response_id'], ),
    sa.ForeignKeyConstraint(['survey_id'], ['survey.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.add_column('ppi_validation_errors', sa.Column('results_id', sa.BigInteger(), nullable=True))
    op.create_foreign_key(None, 'ppi_validation_errors', 'ppi_validation_results', ['results_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'ppi_validation_errors', type_='foreignkey')
    op.drop_column('ppi_validation_errors', 'results_id')
    op.drop_table('ppi_validation_results')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

