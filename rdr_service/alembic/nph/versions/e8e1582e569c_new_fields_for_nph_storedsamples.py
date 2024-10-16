"""New fields for NPH StoredSamples

Revision ID: e8e1582e569c
Revises: aff2e875b4ef
Create Date: 2024-08-21 11:12:36.170010

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e8e1582e569c'
down_revision = 'aff2e875b4ef'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('stored_sample', sa.Column('freeze_thaw_count', sa.Integer(), nullable=True))
    op.add_column('stored_sample', sa.Column('specimen_volume_ul', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('stored_sample', 'sample_volume_ul')
    op.drop_column('stored_sample', 'freeze_thaw_count')
    # ### end Alembic commands ###

