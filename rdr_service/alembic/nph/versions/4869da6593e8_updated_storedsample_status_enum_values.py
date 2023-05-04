"""Updated StoredSample status enum values

Revision ID: 4869da6593e8
Revises: a5c43bf4c6f0
Create Date: 2023-05-04 11:58:47.409409

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '4869da6593e8'
down_revision = 'a5c43bf4c6f0'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    # DISPOSED status
    op.execute(
        """
        UPDATE nph.stored_sample ss SET ss.status = 3 WHERE ss.status = 2
        """
    )
    # SHIPPED status
    op.execute(
        """
        UPDATE nph.stored_sample ss SET ss.status = 2 WHERE ss.status = 1
        """
    )
    # RECEIVED status
    op.execute(
        """
        UPDATE nph.stored_sample ss SET ss.status = 1 WHERE ss.status = 0
        """
    )
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    # DISPOSED status
    op.execute(
        """
        UPDATE nph.stored_sample ss SET ss.status = 0 WHERE ss.status = 1
        """
    )
    # SHIPPED status
    op.execute(
        """
        UPDATE nph.stored_sample ss SET ss.status = 1 WHERE ss.status = 2
        """
    )
    # RECEIVED status
    op.execute(
        """
        UPDATE nph.stored_sample ss SET ss.status = 2 WHERE ss.status = 3
        """
    )
    # ### end Alembic commands ###
