"""remove_wf_state_tracking

Revision ID: 92985871d833
Revises: 1929562f5d92
Create Date: 2023-04-03 10:08:11.790254

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '92985871d833'
down_revision = '1929562f5d92'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("genomic_result_workflow_state")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
