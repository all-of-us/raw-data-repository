"""add authored column

Revision ID: 648d99c5c414
Revises: 80d36c1e37e2
Create Date: 2019-03-08 11:25:48.367648

"""
import model.utils
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "648d99c5c414"
down_revision = "dd408c868dc6"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("questionnaire_response", sa.Column("authored", model.utils.UTCDateTime(), nullable=True))
    # ### end Alembic commands ###
    op.execute("ALTER TABLE questionnaire_response CHANGE COLUMN `authored` `authored` DATETIME NULL AFTER created")


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("questionnaire_response", "authored")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
