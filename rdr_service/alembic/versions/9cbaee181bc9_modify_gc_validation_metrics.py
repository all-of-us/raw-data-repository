"""modify gc validation metrics

Revision ID: 9cbaee181bc9
Revises: 8cda4ff4eba7
Create Date: 2020-03-12 15:12:20.131031

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = '9cbaee181bc9'
down_revision = '8cda4ff4eba7'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('chipwellbarcode', sa.String(length=80), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_green_received', sa.SmallInteger(), nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_red_received', sa.SmallInteger(), nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('tbi_received', sa.SmallInteger(), nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_received', sa.SmallInteger(), nullable=False))
    op.drop_column('genomic_gc_validation_metrics', 'biobank_id')
    op.drop_column('genomic_gc_validation_metrics', 'sample_id')
    # ### end Alembic commands ###

    # Change datatypes
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `call_rate` VARCHAR(10);')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `mean_coverage` VARCHAR(10);')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `genome_coverage` VARCHAR(10);')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `contamination` VARCHAR(10);')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `site_id` VARCHAR(80);')


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('sample_id', mysql.VARCHAR(length=80), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('biobank_id', mysql.VARCHAR(length=80), nullable=False))
    op.drop_column('genomic_gc_validation_metrics', 'vcf_received')
    op.drop_column('genomic_gc_validation_metrics', 'tbi_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_red_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_green_received')
    op.drop_column('genomic_gc_validation_metrics', 'chipwellbarcode')
    # ### end Alembic commands ###

    # Change datatypes
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `call_rate` INTEGER;')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `mean_coverage` INTEGER;')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `genome_coverage` INTEGER;')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `contamination` INTEGER;')
    op.execute('ALTER TABLE genomic_gc_validation_metrics MODIFY `site_id` INTEGER;')


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
