"""Add summary columns

Revision ID: f2aa951ca1a7
Revises: 7e250583b9cb
Create Date: 2017-10-23 16:50:06.586388

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.participant_enums import OrderStatus

# revision identifiers, used by Alembic.
revision = 'f2aa951ca1a7'
down_revision = '7e250583b9cb'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('biospecimen_collected_site_id', sa.Integer(), nullable=True))
    op.add_column('participant_summary', sa.Column('biospecimen_finalized_site_id', sa.Integer(), nullable=True))
    op.add_column('participant_summary', sa.Column('biospecimen_order_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('biospecimen_processed_site_id', sa.Integer(), nullable=True))
    op.add_column('participant_summary', sa.Column('biospecimen_source_site_id', sa.Integer(), nullable=True))
    op.add_column('participant_summary', sa.Column('biospecimen_status', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('physical_measurements_created_site_id', sa.Integer(), nullable=True))
    op.add_column('participant_summary', sa.Column('physical_measurements_finalized_site_id', sa.Integer(), nullable=True))
    op.add_column('participant_summary', sa.Column('physical_measurements_finalized_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ed04', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ed04_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ed10', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ed10_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1hep4', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1hep4_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1pst8', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1pst8_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1sal', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1sal_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1sst8', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1sst8_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ur10', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_1ur10_time', model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_2ed10', model.utils.Enum(OrderStatus), nullable=True))
    op.add_column('participant_summary', sa.Column('sample_order_status_2ed10_time', model.utils.UTCDateTime(), nullable=True))
    op.create_foreign_key(None, 'participant_summary', 'site', ['biospecimen_processed_site_id'], ['site_id'])
    op.create_foreign_key(None, 'participant_summary', 'site', ['physical_measurements_finalized_site_id'], ['site_id'])
    op.create_foreign_key(None, 'participant_summary', 'site', ['physical_measurements_created_site_id'], ['site_id'])
    op.create_foreign_key(None, 'participant_summary', 'site', ['biospecimen_collected_site_id'], ['site_id'])
    op.create_foreign_key(None, 'participant_summary', 'site', ['biospecimen_source_site_id'], ['site_id'])
    op.create_foreign_key(None, 'participant_summary', 'site', ['biospecimen_finalized_site_id'], ['site_id'])
    op.add_column('physical_measurements', sa.Column('finalized', model.utils.UTCDateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('physical_measurements', 'finalized')
    op.drop_constraint(None, 'participant_summary', type_='foreignkey')
    op.drop_constraint(None, 'participant_summary', type_='foreignkey')
    op.drop_constraint(None, 'participant_summary', type_='foreignkey')
    op.drop_constraint(None, 'participant_summary', type_='foreignkey')
    op.drop_constraint(None, 'participant_summary', type_='foreignkey')
    op.drop_constraint(None, 'participant_summary', type_='foreignkey')
    op.drop_column('participant_summary', 'sample_order_status_2ed10_time')
    op.drop_column('participant_summary', 'sample_order_status_2ed10')
    op.drop_column('participant_summary', 'sample_order_status_1ur10_time')
    op.drop_column('participant_summary', 'sample_order_status_1ur10')
    op.drop_column('participant_summary', 'sample_order_status_1sst8_time')
    op.drop_column('participant_summary', 'sample_order_status_1sst8')
    op.drop_column('participant_summary', 'sample_order_status_1sal_time')
    op.drop_column('participant_summary', 'sample_order_status_1sal')
    op.drop_column('participant_summary', 'sample_order_status_1pst8_time')
    op.drop_column('participant_summary', 'sample_order_status_1pst8')
    op.drop_column('participant_summary', 'sample_order_status_1hep4_time')
    op.drop_column('participant_summary', 'sample_order_status_1hep4')
    op.drop_column('participant_summary', 'sample_order_status_1ed10_time')
    op.drop_column('participant_summary', 'sample_order_status_1ed10')
    op.drop_column('participant_summary', 'sample_order_status_1ed04_time')
    op.drop_column('participant_summary', 'sample_order_status_1ed04')
    op.drop_column('participant_summary', 'physical_measurements_finalized_time')
    op.drop_column('participant_summary', 'physical_measurements_finalized_site_id')
    op.drop_column('participant_summary', 'physical_measurements_created_site_id')
    op.drop_column('participant_summary', 'biospecimen_status')
    op.drop_column('participant_summary', 'biospecimen_source_site_id')
    op.drop_column('participant_summary', 'biospecimen_processed_site_id')
    op.drop_column('participant_summary', 'biospecimen_order_time')
    op.drop_column('participant_summary', 'biospecimen_finalized_site_id')
    op.drop_column('participant_summary', 'biospecimen_collected_site_id')
    # ### end Alembic commands ###
