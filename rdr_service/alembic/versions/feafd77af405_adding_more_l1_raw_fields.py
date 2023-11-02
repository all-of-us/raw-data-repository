"""adding_more_l1_raw_fields

Revision ID: feafd77af405
Revises: 4dddd51685cb, 8ad884d3082b
Create Date: 2023-11-02 15:54:23.429892

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'feafd77af405'
down_revision = ('4dddd51685cb', '8ad884d3082b')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_l1_raw', sa.Column('total_concentration_ng_ul', sa.String(length=255), nullable=True))
    op.add_column('genomic_l1_raw', sa.Column('total_dna_ng', sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_l1_raw', 'total_dna_ng')
    op.drop_column('genomic_l1_raw', 'total_concentration_ng_ul')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

