"""initial_sms_tables

Revision ID: a5c43bf4c6f0
Revises: 38399cbaf462
Create Date: 2023-04-25 13:42:56.027363

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'a5c43bf4c6f0'
down_revision = '38399cbaf462'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('sms_blocklist',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=1024), nullable=True),
    sa.Column('identifier_value', sa.BigInteger(), nullable=True),
    sa.Column('identifier_type', sa.String(length=128), nullable=True),
    sa.Column('block_category', sa.String(length=128), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_index(op.f('ix_nph_sms_blocklist_block_category'), 'sms_blocklist',
                    ['block_category'], unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_blocklist_identifier_type'), 'sms_blocklist',
                    ['identifier_type'], unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_blocklist_identifier_value'), 'sms_blocklist',
                    ['identifier_value'], unique=False, schema='nph')
    op.create_table('sms_job_run',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=1024), nullable=True),
    sa.Column('job', sa.String(length=64), nullable=True),
    sa.Column('sub_process', sa.String(length=64), nullable=True),
    sa.Column('result', sa.String(length=64), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_index(op.f('ix_nph_sms_job_run_job'), 'sms_job_run', ['job'],
                    unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_job_run_sub_process'), 'sms_job_run',
                    ['sub_process'], unique=False, schema='nph')
    op.create_table('sms_n0',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=1024), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('job_run_id', sa.BigInteger(), nullable=True),
    sa.Column('lims_sample_id', sa.String(length=32), nullable=True),
    sa.Column('matrix_id', sa.String(length=32), nullable=True),
    sa.Column('biobank_id', sa.String(length=32), nullable=True),
    sa.Column('sample_id', sa.BigInteger(), nullable=True),
    sa.Column('study', sa.String(length=64), nullable=True),
    sa.Column('visit', sa.String(length=64), nullable=True),
    sa.Column('timepoint', sa.String(length=64), nullable=True),
    sa.Column('collection_site', sa.String(length=16), nullable=True),
    sa.Column('collection_date_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('sample_type', sa.String(length=32), nullable=True),
    sa.Column('additive_treatment', sa.String(length=16), nullable=True),
    sa.Column('quantity_ml', sa.String(length=16), nullable=True),
    sa.Column('manufacturer_lot', sa.String(length=32), nullable=True),
    sa.Column('well_box_position', sa.String(length=32), nullable=True),
    sa.Column('storage_unit_id', sa.String(length=32), nullable=True),
    sa.Column('package_id', sa.String(length=32), nullable=True),
    sa.Column('tracking_number', sa.String(length=32), nullable=True),
    sa.Column('shipment_storage_temperature', sa.String(length=16), nullable=True),
    sa.Column('sample_comments', sa.String(length=1024), nullable=True),
    sa.Column('age', sa.String(length=4), nullable=True),
    sa.ForeignKeyConstraint(['job_run_id'], ['nph.sms_job_run.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_index(op.f('ix_nph_sms_n0_file_path'), 'sms_n0', ['file_path'], unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_n0_sample_id'), 'sms_n0', ['sample_id'], unique=False, schema='nph')
    op.create_table('sms_n1_mcac',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=1024), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('job_run_id', sa.BigInteger(), nullable=True),
    sa.Column('sample_id', sa.BigInteger(), nullable=True),
    sa.Column('matrix_id', sa.String(length=32), nullable=True),
    sa.Column('biobank_id', sa.String(length=32), nullable=True),
    sa.Column('sample_identifier', sa.String(length=32), nullable=True),
    sa.Column('study', sa.String(length=64), nullable=True),
    sa.Column('visit', sa.String(length=64), nullable=True),
    sa.Column('timepoint', sa.String(length=64), nullable=True),
    sa.Column('collection_site', sa.String(length=16), nullable=True),
    sa.Column('collection_date_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('sample_type', sa.String(length=32), nullable=True),
    sa.Column('additive_treatment', sa.String(length=16), nullable=True),
    sa.Column('quantity_ml', sa.String(length=16), nullable=True),
    sa.Column('age', sa.String(length=4), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=16), nullable=True),
    sa.Column('package_id', sa.String(length=32), nullable=True),
    sa.Column('storage_unit_id', sa.String(length=32), nullable=True),
    sa.Column('well_box_position', sa.String(length=32), nullable=True),
    sa.Column('destination', sa.String(length=16), nullable=True),
    sa.Column('tracking_number', sa.String(length=32), nullable=True),
    sa.Column('sample_comments', sa.String(length=1024), nullable=True),
    sa.Column('ethnicity', sa.String(length=1024), nullable=True),
    sa.Column('race', sa.String(length=1024), nullable=True),
    sa.Column('bmi', sa.String(length=4), nullable=True),
    sa.Column('diet', sa.String(length=32), nullable=True),
    sa.Column('urine_color', sa.String(length=1024), nullable=True),
    sa.Column('urine_clarity', sa.String(length=1024), nullable=True),
    sa.Column('bowel_movement', sa.String(length=1024), nullable=True),
    sa.Column('bowel_movement_quality', sa.String(length=1024), nullable=True),
    sa.ForeignKeyConstraint(['job_run_id'], ['nph.sms_job_run.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_index(op.f('ix_nph_sms_n1_mcac_file_path'), 'sms_n1_mcac', ['file_path'], unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_n1_mcac_sample_id'), 'sms_n1_mcac', ['sample_id'], unique=False, schema='nph')
    op.create_table('sms_sample',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=1024), nullable=True),
    sa.Column('job_run_id', sa.BigInteger(), nullable=True),
    sa.Column('sample_id', sa.BigInteger(), nullable=True),
    sa.Column('lims_sample_id', sa.String(length=32), nullable=True),
    sa.Column('plate_number', sa.String(length=32), nullable=True),
    sa.Column('position', sa.String(length=16), nullable=True),
    sa.Column('labware_type', sa.String(length=32), nullable=True),
    sa.Column('sample_identifier', sa.String(length=32), nullable=True),
    sa.Column('diet', sa.String(length=32), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=32), nullable=True),
    sa.Column('bmi', sa.String(length=4), nullable=True),
    sa.Column('age', sa.String(length=4), nullable=True),
    sa.Column('race', sa.String(length=1024), nullable=True),
    sa.Column('ethnicity', sa.String(length=1024), nullable=True),
    sa.Column('destination', sa.String(length=64), nullable=True),
    sa.ForeignKeyConstraint(['job_run_id'], ['nph.sms_job_run.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_index(op.f('ix_nph_sms_sample_destination'), 'sms_sample', ['destination'], unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_sample_lims_sample_id'), 'sms_sample',
                    ['lims_sample_id'], unique=False, schema='nph')
    op.create_index(op.f('ix_nph_sms_sample_sample_id'), 'sms_sample', ['sample_id'], unique=False, schema='nph')
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_nph_sms_sample_sample_id'), table_name='sms_sample', schema='nph')
    op.drop_index(op.f('ix_nph_sms_sample_lims_sample_id'), table_name='sms_sample', schema='nph')
    op.drop_index(op.f('ix_nph_sms_sample_destination'), table_name='sms_sample', schema='nph')
    op.drop_table('sms_sample', schema='nph')
    op.drop_index(op.f('ix_nph_sms_n1_mcc_file_path'), table_name='sms_n1_mcc', schema='nph')
    op.drop_table('sms_n1_mcc', schema='nph')
    op.drop_index(op.f('ix_nph_sms_n1_mcac_sample_id'), table_name='sms_n1_mcac', schema='nph')
    op.drop_index(op.f('ix_nph_sms_n1_mcac_file_path'), table_name='sms_n1_mcac', schema='nph')
    op.drop_table('sms_n1_mcac', schema='nph')
    op.drop_index(op.f('ix_nph_sms_n0_sample_id'), table_name='sms_n0', schema='nph')
    op.drop_index(op.f('ix_nph_sms_n0_file_path'), table_name='sms_n0', schema='nph')
    op.drop_table('sms_n0', schema='nph')
    op.drop_index(op.f('ix_nph_sms_job_run_sub_process'), table_name='sms_job_run', schema='nph')
    op.drop_index(op.f('ix_nph_sms_job_run_job'), table_name='sms_job_run', schema='nph')
    op.drop_table('sms_job_run', schema='nph')
    op.drop_index(op.f('ix_nph_sms_blocklist_identifier_value'), table_name='sms_blocklist', schema='nph')
    op.drop_index(op.f('ix_nph_sms_blocklist_identifier_type'), table_name='sms_blocklist', schema='nph')
    op.drop_index(op.f('ix_nph_sms_blocklist_block_category'), table_name='sms_blocklist', schema='nph')
    op.drop_table('sms_blocklist', schema='nph')
    # ### end Alembic commands ###
