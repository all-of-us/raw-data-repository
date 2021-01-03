"""survey models

Revision ID: ee6e75d3e41b
Revises: 50d9eeb498c3
Create Date: 2020-12-31 13:42:52.538559

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
revision = 'ee6e75d3e41b'
down_revision = '50d9eeb498c3'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('survey',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('code_id', sa.Integer(), nullable=True),
    sa.Column('import_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('replaced_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('redcap_project_id', sa.Integer(), nullable=True),
    sa.Column('redcap_project_title', sa.String(length=200), nullable=True),
    sa.ForeignKeyConstraint(['code_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('survey_question',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('code_id', sa.Integer(), nullable=True),
    sa.Column('survey_id', sa.Integer(), nullable=True),
    sa.Column('type', sa.String(length=200), nullable=True),
    sa.Column('validation', sa.String(length=200), nullable=True),
    sa.Column('display', sa.String(length=200), nullable=True),
    sa.ForeignKeyConstraint(['code_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['survey_id'], ['survey.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('survey_question_option',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('code_id', sa.Integer(), nullable=True),
    sa.Column('question_id', sa.Integer(), nullable=True),
    sa.Column('display', sa.String(length=200), nullable=True),
    sa.ForeignKeyConstraint(['code_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['question_id'], ['survey_question.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('survey_question_option')
    op.drop_table('survey_question')
    op.drop_table('survey')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

