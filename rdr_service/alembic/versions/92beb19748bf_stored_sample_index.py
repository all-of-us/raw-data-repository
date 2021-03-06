"""stored sample index

Revision ID: 92beb19748bf
Revises: 0041519860c1
Create Date: 2019-06-25 10:02:03.141799

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "92beb19748bf"
down_revision = "0041519860c1"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("create index ix_boi_test on biobank_stored_sample (biobank_order_identifier, test)")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("drop index ix_boi_test on biobank_stored_sample")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
