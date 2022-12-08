"""adding streetAddress2 field

Revision ID: e3272c2dbf9a
Revises: 58e8df756d74
Create Date: 2019-01-30 12:39:41.452800

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e3272c2dbf9a"
down_revision = "58e8df756d74"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("participant_summary", sa.Column("street_address2", sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("participant_summary", "street_address2")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
