"""fix dv order created

Revision ID: dc971fc16861
Revises: 4ca75174f417
Create Date: 2019-03-15 14:19:38.119583

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'dc971fc16861'
down_revision = '4ca75174f417'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('biobank_dv_order', 'created',
               existing_type=mysql.DATETIME(fsp=6),
               nullable=True,
               existing_server_default=sa.text(u'current_timestamp(6)'))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('biobank_dv_order', 'created',
               existing_type=mysql.DATETIME(fsp=6),
               nullable=False,
               existing_server_default=sa.text(u'current_timestamp(6)'))
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

