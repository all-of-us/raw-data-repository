"""Add Emotional and Behavioral Health columns to Participant Summary

Revision ID: 3d95302cfb96
Revises: 44e4e5cf98ee
Create Date: 2023-06-20 15:20:13.051127

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
revision = '3d95302cfb96'
down_revision = '44e4e5cf98ee'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('questionnaire_on_behavioral_health', rdr_service.model.utils.Enum(QuestionnaireStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_behavioral_health_authored', rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_behavioral_health_time', rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_emotional_health', rdr_service.model.utils.Enum(QuestionnaireStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_emotional_health_authored', rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_emotional_health_time', rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.drop_column('participant_summary', 'questionnaire_on_behavioral_health_and_personality')
    op.drop_column('participant_summary', 'questionnaire_on_emotional_health_history_and_well_being')
    op.drop_column('participant_summary', 'questionnaire_on_behavioral_health_and_personality_authored')
    op.drop_column('participant_summary', 'questionnaire_on_behavioral_health_and_personality_time')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('questionnaire_on_behavioral_health_and_personality_time', mysql.DATETIME(), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_behavioral_health_and_personality_authored', mysql.DATETIME(), nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_emotional_health_history_and_well_being', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=True))
    op.add_column('participant_summary', sa.Column('questionnaire_on_behavioral_health_and_personality', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=True))
    op.drop_column('participant_summary', 'questionnaire_on_emotional_health_time')
    op.drop_column('participant_summary', 'questionnaire_on_emotional_health_authored')
    op.drop_column('participant_summary', 'questionnaire_on_emotional_health')
    op.drop_column('participant_summary', 'questionnaire_on_behavioral_health_time')
    op.drop_column('participant_summary', 'questionnaire_on_behavioral_health_authored')
    op.drop_column('participant_summary', 'questionnaire_on_behavioral_health')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

