"""add cancelled to PM

Revision ID: 2b328e8e5eb8
Revises: b4f6eb55d503
Create Date: 2018-09-25 14:27:46.744408

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.participant_enums import PhysicalMeasurementsStatus

# revision identifiers, used by Alembic.
revision = '2b328e8e5eb8'
down_revision = 'b4f6eb55d503'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('physical_measurements', sa.Column('cancelled_site_id', sa.Integer(), nullable=True))
    op.add_column('physical_measurements', sa.Column('cancelled_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('physical_measurements', sa.Column('cancelled_username', sa.String(length=255), nullable=True))
    op.add_column('physical_measurements', sa.Column('status', model.utils.Enum(PhysicalMeasurementsStatus), nullable=True))
    op.create_foreign_key(None, 'physical_measurements', 'site', ['cancelled_site_id'], ['site_id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'physical_measurements', type_='foreignkey')
    op.drop_column('physical_measurements', 'status')
    op.drop_column('physical_measurements', 'cancelled_username')
    op.drop_column('physical_measurements', 'cancelled_time')
    op.drop_column('physical_measurements', 'cancelled_site_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

