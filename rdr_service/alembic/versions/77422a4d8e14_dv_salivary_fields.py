"""DV salivary fields

Revision ID: 77422a4d8e14
Revises: 4825a0ad42e1
Create Date: 2019-03-05 10:03:54.040675

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.participant_enums import SampleStatus

# revision identifiers, used by Alembic.
revision = "77422a4d8e14"
down_revision = "fba05450d70b"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "participant_summary", sa.Column("sample_status_dv_1sal2", model.utils.Enum(SampleStatus), nullable=True)
    )
    op.add_column(
        "participant_summary", sa.Column("sample_status_dv_1sal2_time", model.utils.UTCDateTime(), nullable=True)
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("participant_summary", "sample_status_dv_1sal2_time")
    op.drop_column("participant_summary", "sample_status_dv_1sal2")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
