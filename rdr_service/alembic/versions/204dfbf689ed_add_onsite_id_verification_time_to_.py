"""Add onsite_id_verification_time to participant_summary

Revision ID: 204dfbf689ed
Revises: 780892c15486
Create Date: 2022-07-11 14:29:38.392818

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


# revision identifiers, used by Alembic.
revision = '204dfbf689ed'
down_revision = '780892c15486'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('onsite_id_verification_time', rdr_service.model.utils.UTCDateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'onsite_id_verification_time')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
