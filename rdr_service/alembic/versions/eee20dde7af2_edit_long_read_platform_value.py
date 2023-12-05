"""edit_long_read_platform_value

Revision ID: eee20dde7af2
Revises: 41020a7cb76c, f8f8ed9b0122, 9dbb2902ea7c
Create Date: 2023-11-30 15:58:28.384804

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'eee20dde7af2'
down_revision = ('41020a7cb76c', 'f8f8ed9b0122', '9dbb2902ea7c')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        """
        AlTER TABLE genomic_long_read modify long_read_platform int
        """
    )
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
