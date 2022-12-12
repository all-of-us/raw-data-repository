"""bigquery sync

Revision ID: 484c5d15ac06
Revises: b662c5bb00cc
Create Date: 2019-05-31 16:22:55.821536

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "484c5d15ac06"
down_revision = "b662c5bb00cc"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "bigquery_sync",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("modified", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("dataset", sa.String(length=80), nullable=False),
        sa.Column("table", sa.String(length=80), nullable=False),
        sa.Column("participant_id", sa.Integer(), nullable=False),
        sa.Column("resource", mysql.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["participant_id"], ["participant.participant_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_bigquery_sync_created"), "bigquery_sync", ["created"], unique=False)
    op.create_index(op.f("ix_bigquery_sync_modified"), "bigquery_sync", ["modified"], unique=False)
    op.create_index("ix_participant_ds_table", "bigquery_sync", ["participant_id", "dataset", "table"], unique=False)

    op.execute("ALTER TABLE patient_status CHANGE COLUMN `created` `created` DATETIME(6) NULL;")
    op.execute("ALTER TABLE patient_status CHANGE COLUMN `modified` `modified` DATETIME(6) NULL")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_ds_table", table_name="bigquery_sync")
    op.drop_index(op.f("ix_bigquery_sync_modified"), table_name="bigquery_sync")
    op.drop_index(op.f("ix_bigquery_sync_created"), table_name="bigquery_sync")
    op.drop_table("bigquery_sync")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
