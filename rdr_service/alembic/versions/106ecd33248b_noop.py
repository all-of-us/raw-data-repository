"""Participant views

Revision ID: 106ecd33248b
Revises: fddd3f850e2c
Create Date: 2018-01-08 11:24:41.814336

"""

# revision identifiers, used by Alembic.
revision = "106ecd33248b"
down_revision = "fddd3f850e2c"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    pass


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
