"""adding_p3_raw

Revision ID: cf98333d3202
Revises: d3ab710e8e87
Create Date: 2024-04-16 15:51:32.762150

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
revision = 'cf98333d3202'
down_revision = 'd3ab710e8e87'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_p3_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('biobankid_sampleid', sa.String(length=1028), nullable=True),
    sa.Column('research_id', sa.String(length=1028), nullable=True),
    sa.Column('lims_id', sa.String(length=1028), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=1028), nullable=True),
    sa.Column('site_id', sa.String(length=1028), nullable=True),
    sa.Column('sample_source', sa.String(length=1028), nullable=True),
    sa.Column('genome_type', sa.String(length=1028), nullable=True),
    sa.Column('ai_an', sa.String(length=1028), nullable=True),
    sa.Column('software_version', sa.String(length=1028), nullable=True),
    sa.Column('npx_explore_path', sa.String(length=1028), nullable=True),
    sa.Column('analysis_report_path', sa.String(length=1028), nullable=True),
    sa.Column('kit_type', sa.String(length=1028), nullable=True),
    sa.Column('notes', sa.String(length=1028), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_p3_raw_biobank_id'), 'genomic_p3_raw', ['biobank_id'], unique=False)
    op.create_index(op.f('ix_genomic_p3_raw_file_path'), 'genomic_p3_raw', ['file_path'], unique=False)
    op.create_index(op.f('ix_genomic_p3_raw_sample_id'), 'genomic_p3_raw', ['sample_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_genomic_p3_raw_sample_id'), table_name='genomic_p3_raw')
    op.drop_index(op.f('ix_genomic_p3_raw_file_path'), table_name='genomic_p3_raw')
    op.drop_index(op.f('ix_genomic_p3_raw_biobank_id'), table_name='genomic_p3_raw')
    op.drop_table('genomic_p3_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

