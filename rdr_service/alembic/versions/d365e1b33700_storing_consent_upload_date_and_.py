"""storing consent upload date and expected signing date

Revision ID: d365e1b33700
Revises: e9a549d1882d
Create Date: 2021-06-14 10:21:51.289320

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


# revision identifiers, used by Alembic.
revision = 'd365e1b33700'
down_revision = 'e9a549d1882d'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('consent_file', sa.Column('expected_sign_date', sa.Date(), nullable=True))
    op.add_column('consent_file', sa.Column('file_upload_time', rdr_service.model.utils.UTCDateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('consent_file', 'file_upload_time')
    op.drop_column('consent_file', 'expected_sign_date')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

