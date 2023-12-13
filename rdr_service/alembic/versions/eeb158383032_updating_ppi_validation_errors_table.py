"""Updating PPI Validation Errors Table

Revision ID: eeb158383032
Revises: 7bed6c17365e
Create Date: 2023-12-07 12:23:15.675494

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
revision = 'eeb158383032'
down_revision = '7bed6c17365e'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('ppi_validation_errors', sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False))
    op.add_column('ppi_validation_errors', sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.add_column('ppi_validation_errors', sa.Column('questionnaire_response_id', sa.Integer(), nullable=False))
    op.add_column('ppi_validation_errors', sa.Column('survey_code_id', sa.Integer(), nullable=False))
    op.create_foreign_key(None, 'ppi_validation_errors', 'questionnaire_response_answer', ['questionnaire_response_answer_id'], ['questionnaire_response_answer_id'])
    op.create_foreign_key(None, 'ppi_validation_errors', 'code', ['survey_code_id'], ['code_id'])
    op.create_foreign_key(None, 'ppi_validation_errors', 'questionnaire_response', ['questionnaire_response_id'], ['questionnaire_response_id'])
    op.drop_column('ppi_validation_errors', 'eval_date')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('ppi_validation_errors', sa.Column('eval_date', mysql.DATETIME(), nullable=False))
    op.drop_constraint(None, 'ppi_validation_errors', type_='foreignkey')
    op.drop_constraint(None, 'ppi_validation_errors', type_='foreignkey')
    op.drop_constraint(None, 'ppi_validation_errors', type_='foreignkey')
    op.drop_column('ppi_validation_errors', 'survey_code_id')
    op.drop_column('ppi_validation_errors', 'questionnaire_response_id')
    op.drop_column('ppi_validation_errors', 'modified')
    op.drop_column('ppi_validation_errors', 'created')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
