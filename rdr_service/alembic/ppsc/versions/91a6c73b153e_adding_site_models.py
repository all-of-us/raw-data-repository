"""adding_site_models

Revision ID: 91a6c73b153e
Revises: 54b168ac16d8
Create Date: 2024-08-07 15:12:22.504430

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = '91a6c73b153e'
down_revision = '54b168ac16d8'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_ppsc():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('partner_activity',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('rdr_note', sa.String(length=1024), nullable=True),
    sa.Column('rule_codes', mysql.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_table('site',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('awardee_id', sa.String(length=128), nullable=False),
    sa.Column('org_id', sa.String(length=128), nullable=False),
    sa.Column('site_name', sa.String(length=512), nullable=False),
    sa.Column('site_identifier', sa.String(length=128), nullable=False),
    sa.Column('notes', sa.String(length=512), nullable=True),
    sa.Column('scheduling_instructions', sa.String(length=1028), nullable=True),
    sa.Column('enrollment_status_active', mysql.TINYINT(), nullable=True),
    sa.Column('digital_scheduling_status_active', mysql.TINYINT(), nullable=True),
    sa.Column('scheduling_status_active', mysql.TINYINT(), nullable=True),
    sa.Column('anticipated_launch_date', sa.String(length=128), nullable=True),
    sa.Column('location_name', sa.String(length=512), nullable=True),
    sa.Column('directions', sa.String(length=1028), nullable=True),
    sa.Column('mayo_link_id', sa.String(length=128), nullable=True),
    sa.Column('active', mysql.TINYINT(), nullable=True),
    sa.Column('address_line', sa.String(length=1028), nullable=True),
    sa.Column('city', sa.String(length=128), nullable=True),
    sa.Column('state', sa.String(length=128), nullable=True),
    sa.Column('postal_code', sa.String(length=128), nullable=True),
    sa.Column('phone', sa.String(length=128), nullable=True),
    sa.Column('email', sa.String(length=512), nullable=True),
    sa.Column('url', sa.String(length=512), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_index(op.f('ix_ppsc_site_awardee_id'), 'site', ['awardee_id'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_org_id'), 'site', ['org_id'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_site_identifier'), 'site', ['site_identifier'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_site_name'), 'site', ['site_name'], unique=False, schema='ppsc')
    op.create_table('partner_event_activity',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=512), nullable=True),
    sa.Column('activity_id', sa.Integer(), nullable=True),
    sa.Column('resource', mysql.JSON(), nullable=True),
    sa.ForeignKeyConstraint(['activity_id'], ['ppsc.partner_activity.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.execute(
        """
        INSERT INTO ppsc.partner_activity (created, modified, name, ignore_flag)
        VALUES(now(), now(), 'Site Update', 0)
        ;
        """
    )
    # ### end Alembic commands ###


def downgrade_ppsc():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('partner_event_activity', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_site_name'), table_name='site', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_site_identifier'), table_name='site', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_org_id'), table_name='site', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_awardee_id'), table_name='site', schema='ppsc')
    op.drop_table('site', schema='ppsc')
    op.drop_table('partner_activity', schema='ppsc')
    # ### end Alembic commands ###
