"""Remove data file Received columns from genomic_gc_validation_metrics

Revision ID: 42428d88dd1d
Revises: f0d635507938, 2ddb58c2b603, ace982c2cb2b, 3bd1cf3a498d
Create Date: 2022-09-16 09:10:07.627665

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql



# revision identifiers, used by Alembic.
revision = '42428d88dd1d'
down_revision = ('f0d635507938', '2ddb58c2b603', 'ace982c2cb2b', '3bd1cf3a498d')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_gc_validation_metrics', 'vcf_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'raw_vcf_tbi_received')
    op.drop_column('genomic_gc_validation_metrics', 'raw_vcf_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_red_received')
    op.drop_column('genomic_gc_validation_metrics', 'vcf_received')
    op.drop_column('genomic_gc_validation_metrics', 'crai_received')
    op.drop_column('genomic_gc_validation_metrics', 'cram_received')
    op.drop_column('genomic_gc_validation_metrics', 'gvcf_received')
    op.drop_column('genomic_gc_validation_metrics', 'gvcf_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'hf_vcf_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_green_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'cram_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'hf_vcf_received')
    op.drop_column('genomic_gc_validation_metrics', 'vcf_tbi_received')
    op.drop_column('genomic_gc_validation_metrics', 'raw_vcf_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_red_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'hf_vcf_tbi_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_green_received')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_green_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('hf_vcf_tbi_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_red_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('raw_vcf_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_tbi_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('hf_vcf_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('cram_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_green_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('hf_vcf_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('gvcf_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('gvcf_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('cram_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('crai_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_red_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('raw_vcf_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('raw_vcf_tbi_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
