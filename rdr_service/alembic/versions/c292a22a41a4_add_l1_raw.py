"""add_l1_raw

Revision ID: c292a22a41a4
Revises: cff00db02aeb
Create Date: 2023-10-17 12:46:11.758781

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
revision = 'c292a22a41a4'
down_revision = 'cff00db02aeb'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_l1_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('package_id', sa.String(length=255), nullable=True),
    sa.Column('biobankid_sampleid', sa.String(length=255), nullable=True),
    sa.Column('box_storageunit_id', sa.String(length=255), nullable=True),
    sa.Column('box_id_plate_id', sa.String(length=255), nullable=True),
    sa.Column('well_position', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('parent_sample_id', sa.String(length=255), nullable=True),
    sa.Column('collection_tubeid', sa.String(length=255), nullable=True),
    sa.Column('matrix_id', sa.String(length=255), nullable=True),
    sa.Column('collection_date', sa.String(length=255), nullable=True),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=255), nullable=True),
    sa.Column('age', sa.String(length=255), nullable=True),
    sa.Column('ny_state_y_n', sa.String(length=255), nullable=True),
    sa.Column('sample_type', sa.String(length=255), nullable=True),
    sa.Column('treatments', sa.String(length=255), nullable=True),
    sa.Column('quantity_ul', sa.String(length=255), nullable=True),
    sa.Column('visit_description', sa.String(length=255), nullable=True),
    sa.Column('sample_source', sa.String(length=255), nullable=True),
    sa.Column('study', sa.String(length=255), nullable=True),
    sa.Column('tracking_number', sa.String(length=255), nullable=True),
    sa.Column('contact', sa.String(length=255), nullable=True),
    sa.Column('email', sa.String(length=255), nullable=True),
    sa.Column('study_pi', sa.String(length=255), nullable=True),
    sa.Column('site_name', sa.String(length=255), nullable=True),
    sa.Column('genome_type', sa.String(length=80), nullable=True),
    sa.Column('lr_site_id', sa.String(length=80), nullable=True),
    sa.Column('long_read_platform', sa.String(length=80), nullable=True),
    sa.Column('failure_mode', sa.String(length=255), nullable=True),
    sa.Column('failure_mode_desc', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_l1_raw_biobank_id'), 'genomic_l1_raw', ['biobank_id'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_collection_tubeid'), 'genomic_l1_raw', ['collection_tubeid'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_file_path'), 'genomic_l1_raw', ['file_path'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_genome_type'), 'genomic_l1_raw', ['genome_type'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_long_read_platform'), 'genomic_l1_raw', ['long_read_platform'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_lr_site_id'), 'genomic_l1_raw', ['lr_site_id'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_parent_sample_id'), 'genomic_l1_raw', ['parent_sample_id'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_sample_id'), 'genomic_l1_raw', ['sample_id'], unique=False)
    op.create_index(op.f('ix_genomic_l1_raw_site_name'), 'genomic_l1_raw', ['site_name'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_genomic_l1_raw_site_name'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_sample_id'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_parent_sample_id'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_lr_site_id'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_long_read_platform'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_genome_type'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_file_path'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_collection_tubeid'), table_name='genomic_l1_raw')
    op.drop_index(op.f('ix_genomic_l1_raw_biobank_id'), table_name='genomic_l1_raw')
    op.drop_table('genomic_l1_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

