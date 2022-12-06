"""add flag for ghost id

Revision ID: 93d831aa6fb4
Revises: e3272c2dbf9a
Create Date: 2019-01-30 14:55:59.604938

"""
import model.utils
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "93d831aa6fb4"
down_revision = "e3272c2dbf9a"
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
    op.add_column("participant", sa.Column("date_added_ghost", model.utils.UTCDateTime(), nullable=True))
    op.add_column("participant", sa.Column("is_ghost_id", sa.Boolean(), nullable=True))
    op.add_column("participant_history", sa.Column("date_added_ghost", model.utils.UTCDateTime(), nullable=True))
    op.add_column("participant_history", sa.Column("is_ghost_id", sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("participant_history", "is_ghost_id")
    op.drop_column("participant_history", "date_added_ghost")
    op.drop_column("participant", "is_ghost_id")
    op.drop_column("participant", "date_added_ghost")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
