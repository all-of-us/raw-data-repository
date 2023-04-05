"""rename deactivated_event table to deactivation_event

Revision ID: 38399cbaf462
Revises: a83d615761a6
Create Date: 2023-04-05 11:04:27.428691

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '38399cbaf462'
down_revision = 'a83d615761a6'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('RENAME TABLE nph.deactivated_event to nph.deactivation_event')



def downgrade_nph():
    op.execute('RENAME TABLE nph.deactivation_event TO nph.deactivated_event')
    # ### end Alembic commands ###
