"""add_diet_string_to_diet_event

Revision ID: 04817233e930
Revises: f016544598f7
Create Date: 2024-02-08 12:50:43.746047

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '04817233e930'
down_revision = 'f016544598f7'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('diet_event', sa.Column('diet_name_str', sa.String(length=128), nullable=True))
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('diet_event', 'diet_name_str')
    # ### end Alembic commands ###

