"""add fingerprint_path to genomic_set_member

Revision ID: f73a5e7b1822
Revises: 0d5e58df7917
Create Date: 2020-11-09 12:17:28.477732

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'f73a5e7b1822'
down_revision = '0d5e58df7917'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_set_member', sa.Column('fingerprint_path', sa.String(length=255), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('fingerprint_path', sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_set_member', 'fingerprint_path')
    op.drop_column('genomic_set_member_history', 'fingerprint_path')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
