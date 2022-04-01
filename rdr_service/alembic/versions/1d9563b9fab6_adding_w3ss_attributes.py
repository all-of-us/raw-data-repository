"""adding_w3ss_attributes

Revision ID: 1d9563b9fab6
Revises: 825eefb014f9
Create Date: 2022-03-31 17:00:15.892225

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '1d9563b9fab6'
down_revision = '825eefb014f9'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_w3ss_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('dev_note', sa.String(length=255), nullable=True),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('packageId', sa.String(length=250), nullable=True),
    sa.Column('version', sa.String(length=255), nullable=True),
    sa.Column('box_storageunit_id', sa.String(length=255), nullable=True),
    sa.Column('box_id_plate_id', sa.String(length=255), nullable=True),
    sa.Column('well_position', sa.String(length=255), nullable=True),
    sa.Column('cvl_sample_id', sa.String(length=255), nullable=True),
    sa.Column('parent_sample_id', sa.String(length=255), nullable=True),
    sa.Column('collection_tube_id', sa.String(length=255), nullable=True),
    sa.Column('matrix_id', sa.String(length=255), nullable=True),
    sa.Column('collection_date', sa.String(length=255), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=255), nullable=True),
    sa.Column('age', sa.String(length=255), nullable=True),
    sa.Column('ny_state', sa.String(length=255), nullable=True),
    sa.Column('sample_type', sa.String(length=255), nullable=True),
    sa.Column('treatments', sa.String(length=255), nullable=True),
    sa.Column('quantity', sa.String(length=255), nullable=True),
    sa.Column('total_concentration', sa.String(length=255), nullable=True),
    sa.Column('total_dna', sa.String(length=255), nullable=True),
    sa.Column('visit_description', sa.String(length=255), nullable=True),
    sa.Column('sample_source', sa.String(length=255), nullable=True),
    sa.Column('study', sa.String(length=255), nullable=True),
    sa.Column('tracking_number', sa.String(length=255), nullable=True),
    sa.Column('contact', sa.String(length=255), nullable=True),
    sa.Column('email', sa.String(length=255), nullable=True),
    sa.Column('study_pi', sa.String(length=255), nullable=True),
    sa.Column('site_name', sa.String(length=255), nullable=True),
    sa.Column('genome_type', sa.String(length=80), nullable=True),
    sa.Column('failure_mode', sa.String(length=255), nullable=True),
    sa.Column('failure_mode_desc', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_w3ss_raw_file_path'), 'genomic_w3ss_raw', ['file_path'], unique=False)
    op.create_index(op.f('ix_genomic_w3ss_raw_genome_type'), 'genomic_w3ss_raw', ['genome_type'], unique=False)
    op.create_index(op.f('ix_genomic_w3ss_raw_site_name'), 'genomic_w3ss_raw', ['site_name'], unique=False)
    op.create_table('genomic_cvl_second_sample',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('genomic_set_member_id', sa.Integer(), nullable=False),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('packageId', sa.String(length=250), nullable=True),
    sa.Column('version', sa.String(length=255), nullable=False),
    sa.Column('box_storageunit_id', sa.String(length=255), nullable=True),
    sa.Column('box_id_plate_id', sa.String(length=255), nullable=True),
    sa.Column('well_position', sa.String(length=255), nullable=True),
    sa.Column('cvl_sample_id', sa.String(length=255), nullable=True),
    sa.Column('parent_sample_id', sa.String(length=255), nullable=True),
    sa.Column('collection_tube_id', sa.String(length=255), nullable=True),
    sa.Column('matrix_id', sa.String(length=255), nullable=True),
    sa.Column('collection_date', sa.String(length=255), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=255), nullable=True),
    sa.Column('age', sa.String(length=255), nullable=True),
    sa.Column('ny_state', sa.String(length=255), nullable=True),
    sa.Column('sample_type', sa.String(length=255), nullable=True),
    sa.Column('treatments', sa.String(length=255), nullable=True),
    sa.Column('quantity', sa.String(length=255), nullable=True),
    sa.Column('total_concentration', sa.String(length=255), nullable=True),
    sa.Column('total_dna', sa.String(length=255), nullable=True),
    sa.Column('visit_description', sa.String(length=255), nullable=True),
    sa.Column('sample_source', sa.String(length=255), nullable=True),
    sa.Column('study', sa.String(length=255), nullable=True),
    sa.Column('tracking_number', sa.String(length=255), nullable=True),
    sa.Column('contact', sa.String(length=255), nullable=True),
    sa.Column('email', sa.String(length=255), nullable=True),
    sa.Column('study_pi', sa.String(length=255), nullable=True),
    sa.Column('site_name', sa.String(length=255), nullable=True),
    sa.Column('genome_type', sa.String(length=80), nullable=True),
    sa.Column('failure_mode', sa.String(length=255), nullable=True),
    sa.Column('failure_mode_desc', sa.String(length=255), nullable=True),
    sa.ForeignKeyConstraint(['genomic_set_member_id'], ['genomic_set_member.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_cvl_second_sample_genomic_set_member_id'), 'genomic_cvl_second_sample', ['genomic_set_member_id'], unique=False)
    op.add_column('genomic_set_member', sa.Column('cvl_w3ss_manifest_job_run_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['cvl_w3ss_manifest_job_run_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.drop_column('genomic_set_member', 'cvl_w3ss_manifest_job_run_id')
    op.drop_index(op.f('ix_genomic_cvl_second_sample_genomic_set_member_id'), table_name='genomic_cvl_second_sample')
    op.drop_table('genomic_cvl_second_sample')
    op.drop_index(op.f('ix_genomic_w3ss_raw_site_name'), table_name='genomic_w3ss_raw')
    op.drop_index(op.f('ix_genomic_w3ss_raw_genome_type'), table_name='genomic_w3ss_raw')
    op.drop_index(op.f('ix_genomic_w3ss_raw_file_path'), table_name='genomic_w3ss_raw')
    op.drop_table('genomic_w3ss_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

