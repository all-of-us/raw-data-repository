"""add enrolling status

Revision ID: 2be6f6d054e8
Revises: f17f0686ea6b
Create Date: 2018-03-21 13:57:41.685020

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.model.site_enums import EnrollingStatus

# revision identifiers, used by Alembic.
revision = '2be6f6d054e8'
down_revision = 'f17f0686ea6b'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('site', sa.Column('enrolling_status', model.utils.Enum(EnrollingStatus), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('site', 'enrolling_status')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

