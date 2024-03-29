"""Add file paths to genomic gc validation metrics


Revision ID: d5d97368b14d
Revises: 4507ede4f552
Create Date: 2020-07-24 14:14:21.478839

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd5d97368b14d'
down_revision = '4507ede4f552'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('crai_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('cram_md5_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('cram_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('hf_vcf_md5_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('hf_vcf_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('hf_vcf_tbi_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_green_md5_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_green_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_red_md5_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_red_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('raw_vcf_md5_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('raw_vcf_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('raw_vcf_tbi_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_md5_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_path', sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_gc_validation_metrics', 'vcf_path')
    op.drop_column('genomic_gc_validation_metrics', 'vcf_md5_path')
    op.drop_column('genomic_gc_validation_metrics', 'raw_vcf_tbi_path')
    op.drop_column('genomic_gc_validation_metrics', 'raw_vcf_path')
    op.drop_column('genomic_gc_validation_metrics', 'raw_vcf_md5_path')
    op.drop_column('genomic_gc_validation_metrics', 'idat_red_path')
    op.drop_column('genomic_gc_validation_metrics', 'idat_red_md5_path')
    op.drop_column('genomic_gc_validation_metrics', 'idat_green_path')
    op.drop_column('genomic_gc_validation_metrics', 'idat_green_md5_path')
    op.drop_column('genomic_gc_validation_metrics', 'hf_vcf_tbi_path')
    op.drop_column('genomic_gc_validation_metrics', 'hf_vcf_path')
    op.drop_column('genomic_gc_validation_metrics', 'hf_vcf_md5_path')
    op.drop_column('genomic_gc_validation_metrics', 'cram_path')
    op.drop_column('genomic_gc_validation_metrics', 'cram_md5_path')
    op.drop_column('genomic_gc_validation_metrics', 'crai_path')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
