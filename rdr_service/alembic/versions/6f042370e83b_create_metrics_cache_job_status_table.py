"""create metrics cache job status table

Revision ID: 6f042370e83b
Revises: e2e06644b211
Create Date: 2019-07-17 16:57:10.332454

"""
import model.utils
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6f042370e83b"
down_revision = "e2e06644b211"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "metrics_cache_job_status",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cache_table_name", sa.String(length=100), nullable=False),
        sa.Column("type", sa.String(length=50), nullable=True),
        sa.Column("in_progress", sa.Boolean(), nullable=False),
        sa.Column("complete", sa.Boolean(create_constraint=False), nullable=False),
        sa.Column("date_inserted", model.utils.UTCDateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("metrics_cache_job_status")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
