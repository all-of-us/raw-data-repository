"""Add ignore_flag column to genomic_manifest_file and feedback

Revision ID: 99fb6b79b5f7
Revises: 50d9eeb498c3
Create Date: 2021-01-04 15:40:41.792027

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '99fb6b79b5f7'
down_revision = '50d9eeb498c3'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_manifest_feedback', sa.Column('ignore_flag', sa.SmallInteger(), nullable=False))
    op.add_column('genomic_manifest_file', sa.Column('ignore_flag', sa.SmallInteger(), nullable=False))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_manifest_file', 'ignore_flag')
    op.drop_column('genomic_manifest_feedback', 'ignore_flag')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
