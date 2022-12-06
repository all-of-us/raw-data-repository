"""Update GC Validation Metrics for new data files.

Revision ID: 8302d4762b9a
Revises: 4331eeb400da
Create Date: 2020-07-23 11:30:33.925002

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '8302d4762b9a'
down_revision = '4331eeb400da'
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
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_green_md5_received', sa.SmallInteger(), nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('idat_red_md5_received', sa.SmallInteger(), nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('vcf_md5_received', sa.SmallInteger(), nullable=False))
    op.drop_column('genomic_gc_validation_metrics', 'crai_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'tbi_received')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('tbi_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.add_column('genomic_gc_validation_metrics', sa.Column('crai_md5_received', mysql.SMALLINT(display_width=6), autoincrement=False, nullable=False))
    op.drop_column('genomic_gc_validation_metrics', 'vcf_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_red_md5_received')
    op.drop_column('genomic_gc_validation_metrics', 'idat_green_md5_received')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
