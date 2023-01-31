"""adding volume units column

Revision ID: b1685b23d87e
Revises: 3d78e7cfe123
Create Date: 2023-01-31 11:30:10.093355

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b1685b23d87e'
down_revision = '3d78e7cfe123'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    op.add_column('ordered_sample', sa.Column('volumeUnits', sa.String(length=128), nullable=True))


def downgrade_nph():
    op.drop_column('ordered_sample', 'volumeUnits')
