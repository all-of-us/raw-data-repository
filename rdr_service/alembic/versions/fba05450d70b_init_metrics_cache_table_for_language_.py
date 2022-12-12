"""init Metrics cache table for Language stratification

Revision ID: fba05450d70b
Revises: 4825a0ad42e1
Create Date: 2019-03-05 12:09:55.144163

"""
import model.utils
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fba05450d70b"
down_revision = "4825a0ad42e1"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "metrics_language_cache",
        sa.Column("date_inserted", model.utils.UTCDateTime(), nullable=False),
        sa.Column("enrollment_status", sa.String(length=50), nullable=False),
        sa.Column("hpo_id", sa.String(length=20), nullable=False),
        sa.Column("hpo_name", sa.String(length=255), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("language_name", sa.String(length=50), nullable=False),
        sa.Column("language_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("date_inserted", "enrollment_status", "hpo_id", "hpo_name", "date", "language_name"),
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("metrics_language_cache")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
