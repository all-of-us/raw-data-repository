"""create genomic_aw3_raw table

Revision ID: c569d65b5ef7
Revises: 978d12edb6c5, afb0333cb471
Create Date: 2022-01-14 15:08:34.998718

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c569d65b5ef7'
down_revision = ('978d12edb6c5', 'afb0333cb471')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_aw3_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('dev_note', sa.String(length=255), nullable=True),
    sa.Column('genome_type', sa.String(length=255), nullable=True),
    sa.Column('chipwellbarcode', sa.String(length=255), nullable=True),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('research_id', sa.String(length=255), nullable=True),
    sa.Column('biobankidsampleid', sa.String(length=255), nullable=True),
    sa.Column('sex_at_birth', sa.String(length=255), nullable=True),
    sa.Column('site_id', sa.String(length=255), nullable=True),
    sa.Column('callrate', sa.String(length=255), nullable=True),
    sa.Column('sex_concordance', sa.String(length=255), nullable=True),
    sa.Column('contamination', sa.String(length=255), nullable=True),
    sa.Column('processing_status', sa.String(length=255), nullable=True),
    sa.Column('mean_coverage', sa.String(length=255), nullable=True),
    sa.Column('sample_source', sa.String(length=255), nullable=True),
    sa.Column('pipeline_id', sa.String(length=255), nullable=True),
    sa.Column('mapped_reads_pct', sa.String(length=255), nullable=True),
    sa.Column('sex_ploidy', sa.String(length=255), nullable=True),
    sa.Column('ai_an', sa.String(length=255), nullable=True),
    sa.Column('blocklisted', sa.String(length=255), nullable=True),
    sa.Column('blocklisted_reason', sa.String(length=255), nullable=True),
    sa.Column('red_idat_path', sa.String(length=255), nullable=True),
    sa.Column('red_idat_md5_path', sa.String(length=255), nullable=True),
    sa.Column('green_idat_path', sa.String(length=255), nullable=True),
    sa.Column('green_idat_md5_path', sa.String(length=255), nullable=True),
    sa.Column('vcf_path', sa.String(length=255), nullable=True),
    sa.Column('vcf_index_path', sa.String(length=255), nullable=True),
    sa.Column('vcf_md5_path', sa.String(length=255), nullable=True),
    sa.Column('vcf_hf_path', sa.String(length=255), nullable=True),
    sa.Column('vcf_hf_index_path', sa.String(length=255), nullable=True),
    sa.Column('vcf_hf_md5_path', sa.String(length=255), nullable=True),
    sa.Column('cram_path', sa.String(length=255), nullable=True),
    sa.Column('cram_md5_path', sa.String(length=255), nullable=True),
    sa.Column('crai_path', sa.String(length=255), nullable=True),
    sa.Column('gvcf_path', sa.String(length=255), nullable=True),
    sa.Column('gvcf_md5_path', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_aw3_raw_biobank_id'), 'genomic_aw3_raw', ['biobank_id'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_blocklisted'), 'genomic_aw3_raw', ['blocklisted'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_chipwellbarcode'), 'genomic_aw3_raw', ['chipwellbarcode'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_file_path'), 'genomic_aw3_raw', ['file_path'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_genome_type'), 'genomic_aw3_raw', ['genome_type'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_research_id'), 'genomic_aw3_raw', ['research_id'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_sample_id'), 'genomic_aw3_raw', ['sample_id'], unique=False)
    op.create_index(op.f('ix_genomic_aw3_raw_site_id'), 'genomic_aw3_raw', ['site_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_genomic_aw3_raw_site_id'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_sample_id'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_research_id'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_genome_type'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_file_path'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_chipwellbarcode'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_blocklisted'), table_name='genomic_aw3_raw')
    op.drop_index(op.f('ix_genomic_aw3_raw_biobank_id'), table_name='genomic_aw3_raw')
    op.drop_table('genomic_aw3_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
