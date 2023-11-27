"""adding_r2_fields

Revision ID: 9dbb2902ea7c
Revises: 9ef555da5d10
Create Date: 2023-11-13 12:53:03.626699

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
revision = '9dbb2902ea7c'
down_revision = '9ef555da5d10'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_r2_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('biobankid_sampleid', sa.String(length=255), nullable=True),
    sa.Column('lims_id', sa.String(length=255), nullable=True),
    sa.Column('sample_source', sa.String(length=255), nullable=True),
    sa.Column('alignment_rate_pct', sa.String(length=255), nullable=True),
    sa.Column('duplication_pct', sa.String(length=255), nullable=True),
    sa.Column('mrna_bases_pct', sa.String(length=255), nullable=True),
    sa.Column('reads_aligned_in_pairs', sa.String(length=255), nullable=True),
    sa.Column('ribosomal_bases_pct', sa.String(length=255), nullable=True),
    sa.Column('median_cv_coverage', sa.String(length=255), nullable=True),
    sa.Column('mean_insert_size', sa.String(length=255), nullable=True),
    sa.Column('rqs', sa.String(length=255), nullable=True),
    sa.Column('genome_type', sa.String(length=255), nullable=True),
    sa.Column('processing_status', sa.String(length=255), nullable=True),
    sa.Column('pipeline_id', sa.String(length=255), nullable=True),
    sa.Column('cram_path', sa.String(length=255), nullable=True),
    sa.Column('cram_md5_path', sa.String(length=255), nullable=True),
    sa.Column('notes', sa.String(length=1028), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_r2_raw_biobank_id'), 'genomic_r2_raw', ['biobank_id'], unique=False)
    op.create_index(op.f('ix_genomic_r2_raw_file_path'), 'genomic_r2_raw', ['file_path'], unique=False)
    op.create_index(op.f('ix_genomic_r2_raw_sample_id'), 'genomic_r2_raw', ['sample_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_genomic_r2_raw_sample_id'), table_name='genomic_r2_raw')
    op.drop_index(op.f('ix_genomic_r2_raw_file_path'), table_name='genomic_r2_raw')
    op.drop_index(op.f('ix_genomic_r2_raw_biobank_id'), table_name='genomic_r2_raw')
    op.drop_table('genomic_r2_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
