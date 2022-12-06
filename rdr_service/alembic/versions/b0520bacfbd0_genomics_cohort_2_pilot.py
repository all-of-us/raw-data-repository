"""genomics cohort 2 pilot

Revision ID: b0520bacfbd0
Revises: c1198b53fdd6
Create Date: 2020-06-17 15:27:05.910908

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.participant_enums import ParticipantCohort, ParticipantCohortPilotFlag
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = 'b0520bacfbd0'
down_revision = 'c1198b53fdd6'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('participant_cohort_pilot',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('participant_id', sa.Integer(), nullable=True),
    sa.Column('consent_date', sa.Date(), nullable=True),
    sa.Column('enrollment_status_core_stored_sample_date', sa.Date(), nullable=True),
    sa.Column('cluster', sa.SmallInteger(), nullable=True),
    sa.Column('participant_cohort', rdr_service.model.utils.Enum(ParticipantCohort), nullable=True),
    sa.Column('participant_cohort_pilot', rdr_service.model.utils.Enum(ParticipantCohortPilotFlag), nullable=True),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_participant_cohort_pilot_created'), 'participant_cohort_pilot', ['created'], unique=False)
    op.create_index('participant_cohort_participantId', 'participant_cohort_pilot', ['participant_id'], unique=False)
    op.add_column('participant_summary', sa.Column('cohort_2_pilot_flag', rdr_service.model.utils.Enum(ParticipantCohortPilotFlag), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'cohort_2_pilot_flag')
    op.drop_index('participant_cohort_participantId', table_name='participant_cohort_pilot')
    op.drop_index(op.f('ix_participant_cohort_pilot_created'), table_name='participant_cohort_pilot')
    op.drop_table('participant_cohort_pilot')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
