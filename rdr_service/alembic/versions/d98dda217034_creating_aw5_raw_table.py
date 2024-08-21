"""Creating AW5 Raw Table

Revision ID: d98dda217034
Revises: 7fc7dc8683a6, c33b3da0905a
Create Date: 2024-08-19 12:38:02.123763

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
revision = 'd98dda217034'
down_revision = ('7fc7dc8683a6', 'c33b3da0905a')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_aw5_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('dev_note', sa.String(length=255), nullable=True),
    sa.Column('genome_type', sa.String(length=255), nullable=True),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('biobank_id_sample_id', sa.String(length=255), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=255), nullable=True),
    sa.Column('site_id', sa.String(length=255), nullable=True),
    sa.Column('red_idat', sa.String(length=255), nullable=True),
    sa.Column('red_idat_md5', sa.String(length=255), nullable=True),
    sa.Column('red_idat_basename', sa.String(length=255), nullable=True),
    sa.Column('red_idat_md5_hash', sa.String(length=255), nullable=True),
    sa.Column('green_idat', sa.String(length=255), nullable=True),
    sa.Column('green_idat_md5', sa.String(length=255), nullable=True),
    sa.Column('green_idat_basename', sa.String(length=255), nullable=True),
    sa.Column('green_idat_md5_hash', sa.String(length=255), nullable=True),
    sa.Column('vcf', sa.String(length=255), nullable=True),
    sa.Column('vcf_index', sa.String(length=255), nullable=True),
    sa.Column('vcf_md5', sa.String(length=255), nullable=True),
    sa.Column('vcf_basename', sa.String(length=255), nullable=True),
    sa.Column('vcf_md5_hash', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_aw5_raw_biobank_id'), 'genomic_aw5_raw', ['biobank_id'], unique=False)
    op.create_index(op.f('ix_genomic_aw5_raw_file_path'), 'genomic_aw5_raw', ['file_path'], unique=False)
    op.create_index(op.f('ix_genomic_aw5_raw_genome_type'), 'genomic_aw5_raw', ['genome_type'], unique=False)
    op.create_index(op.f('ix_genomic_aw5_raw_sample_id'), 'genomic_aw5_raw', ['sample_id'], unique=False)
    op.create_index(op.f('ix_genomic_aw5_raw_site_id'), 'genomic_aw5_raw', ['site_id'], unique=False)
    op.drop_column('physical_measurements', 'meets_core_data_reqs')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('physical_measurements', sa.Column('meets_core_data_reqs', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True))
    op.drop_index(op.f('ix_genomic_aw5_raw_site_id'), table_name='genomic_aw5_raw')
    op.drop_index(op.f('ix_genomic_aw5_raw_sample_id'), table_name='genomic_aw5_raw')
    op.drop_index(op.f('ix_genomic_aw5_raw_genome_type'), table_name='genomic_aw5_raw')
    op.drop_index(op.f('ix_genomic_aw5_raw_file_path'), table_name='genomic_aw5_raw')
    op.drop_index(op.f('ix_genomic_aw5_raw_biobank_id'), table_name='genomic_aw5_raw')
    op.drop_table('genomic_aw5_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

