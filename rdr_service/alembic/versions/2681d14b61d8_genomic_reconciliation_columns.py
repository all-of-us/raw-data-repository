"""genomic reconciliation columns

Revision ID: 2681d14b61d8
Revises: e8df4ef80f31
Create Date: 2019-11-26 13:09:24.743022

"""
from alembic import op
import sqlalchemy as sa
import model.utils
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
revision = '2681d14b61d8'
down_revision = 'e8df4ef80f31'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('biobank_id', sa.String(length=80), nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('reconcile_manifest_job_run_id', sa.Integer(), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('reconcile_sequencing_job_run_id', sa.Integer(), nullable=True))
    op.drop_constraint('genomic_gc_validation_metrics_ibfk_2', 'genomic_gc_validation_metrics', type_='foreignkey')
    op.create_foreign_key(None, 'genomic_gc_validation_metrics', 'genomic_set_member', ['genomic_set_member_id'], ['id'])
    op.drop_column('genomic_gc_validation_metrics', 'participant_id')
    op.drop_constraint('genomic_job_run_ibfk_1', 'genomic_job_run', type_='foreignkey')
    # ### end Alembic commands ###

    op.drop_table('genomic_job')


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_foreign_key('genomic_job_run_ibfk_1', 'genomic_job_run', 'genomic_job', ['job_id'], ['id'])
    op.add_column('genomic_gc_validation_metrics', sa.Column('participant_id', mysql.INTEGER(display_width=11), autoincrement=False, nullable=False))
    op.drop_constraint(None, 'genomic_gc_validation_metrics', type_='foreignkey')
    op.create_foreign_key('genomic_gc_validation_metrics_ibfk_2', 'genomic_gc_validation_metrics', 'participant', ['participant_id'], ['participant_id'])
    op.drop_column('genomic_gc_validation_metrics', 'reconcile_sequencing_job_run_id')
    op.drop_column('genomic_gc_validation_metrics', 'reconcile_manifest_job_run_id')
    op.drop_column('genomic_gc_validation_metrics', 'biobank_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

