"""add mapped_reads_pct to aw2 raw and validation metrics

Revision ID: 88ea1bb98358
Revises: 93a39640573a
Create Date: 2021-10-13 12:09:28.782294

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '88ea1bb98358'
down_revision = '93a39640573a'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_aw2_raw', sa.Column('mapped_reads_pct', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('mapped_reads_pct', sa.String(length=10), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_gc_validation_metrics', 'mapped_reads_pct')
    op.drop_column('genomic_aw2_raw', 'mapped_reads_pct')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
