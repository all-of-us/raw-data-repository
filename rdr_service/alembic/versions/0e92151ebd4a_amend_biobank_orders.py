"""amend biobank orders

Revision ID: 0e92151ebd4a
Revises: 075b9eee88b7
Create Date: 2018-08-15 11:43:57.043915

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.participant_enums import BiobankOrderStatus

# revision identifiers, used by Alembic.
revision = '0e92151ebd4a'
down_revision = '075b9eee88b7'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('biobank_order', sa.Column('amended_biobank_order_id', sa.String(length=80), nullable=True))
    op.add_column('biobank_order', sa.Column('amended_reason', sa.UnicodeText(), nullable=True))
    op.add_column('biobank_order', sa.Column('amended_site_id', sa.Integer(), nullable=True))
    op.add_column('biobank_order', sa.Column('amended_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('biobank_order', sa.Column('amended_username', sa.String(length=255), nullable=True))
    op.add_column('biobank_order', sa.Column('cancelled_site_id', sa.Integer(), nullable=True))
    op.add_column('biobank_order', sa.Column('cancelled_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('biobank_order', sa.Column('cancelled_username', sa.String(length=255), nullable=True))
    op.add_column('biobank_order', sa.Column('last_modified', model.utils.UTCDateTime(), nullable=True))
    op.add_column('biobank_order', sa.Column('order_status', model.utils.Enum(BiobankOrderStatus), nullable=True))
    op.add_column('biobank_order', sa.Column('restored_site_id', sa.Integer(), nullable=True))
    op.add_column('biobank_order', sa.Column('restored_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('biobank_order', sa.Column('restored_username', sa.String(length=255), nullable=True))
    op.create_foreign_key(None, 'biobank_order', 'site', ['amended_site_id'], ['site_id'])
    op.create_foreign_key(None, 'biobank_order', 'site', ['cancelled_site_id'], ['site_id'])
    op.create_foreign_key(None, 'biobank_order', 'site', ['restored_site_id'], ['site_id'])
    op.create_foreign_key(None, 'biobank_order', 'biobank_order', ['amended_biobank_order_id'], ['biobank_order_id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'biobank_order', type_='foreignkey')
    op.drop_constraint(None, 'biobank_order', type_='foreignkey')
    op.drop_constraint(None, 'biobank_order', type_='foreignkey')
    op.drop_constraint(None, 'biobank_order', type_='foreignkey')
    op.drop_column('biobank_order', 'restored_username')
    op.drop_column('biobank_order', 'restored_time')
    op.drop_column('biobank_order', 'restored_site_id')
    op.drop_column('biobank_order', 'order_status')
    op.drop_column('biobank_order', 'last_modified')
    op.drop_column('biobank_order', 'cancelled_username')
    op.drop_column('biobank_order', 'cancelled_time')
    op.drop_column('biobank_order', 'cancelled_site_id')
    op.drop_column('biobank_order', 'amended_username')
    op.drop_column('biobank_order', 'amended_time')
    op.drop_column('biobank_order', 'amended_site_id')
    op.drop_column('biobank_order', 'amended_reason')
    op.drop_column('biobank_order', 'amended_biobank_order_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

