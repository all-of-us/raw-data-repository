"""add base intake tables

Revision ID: 83450fb6b452
Revises: 21a92ab43b53
Create Date: 2023-01-12 15:49:31.858452

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '83450fb6b452'
down_revision = '21a92ab43b53'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('activity',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('rdr_note', sa.String(length=1024), nullable=True),
    sa.Column('rule_codes', mysql.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('consent_event_type',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('disable_flag', mysql.TINYINT(), nullable=True),
    sa.Column('disable_reason', sa.String(length=1024), nullable=True),
    sa.Column('name', sa.String(length=1024), nullable=True),
    sa.Column('rule_codes', mysql.JSON(), nullable=True),
    sa.Column('version', sa.String(length=128), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('enrollment_event_type',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('rule_codes', mysql.JSON(), nullable=True),
    sa.Column('version', sa.String(length=128), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('pairing_event_type',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('name', sa.String(length=1024), nullable=True),
    sa.Column('rule_codes', mysql.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('participant_event_activity',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('activity_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['activity_id'], ['nph.activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['nph.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('consent_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_id', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['event_type_id'], ['nph.consent_event_type.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('enrollment_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_id', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['event_type_id'], ['nph.enrollment_event_type.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('pairing_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_id', sa.BigInteger(), nullable=True),
    sa.Column('site_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['event_type_id'], ['nph.pairing_event_type.id'], ),
    sa.ForeignKeyConstraint(['site_id'], ['nph.site.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.drop_constraint('order_ibfk_2', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_5', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_6', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_4', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_1', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_3', 'order', type_='foreignkey')
    op.create_foreign_key(None, 'order', 'site', ['amended_site'], ['id'],
                          source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'order', 'participant', ['participant_id'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['finalized_site'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.create_foreign_key(None, 'order', 'study_category', ['category_id'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['created_site'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['collected_site'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.drop_constraint('ordered_sample_ibfk_1', 'ordered_sample', type_='foreignkey')
    op.drop_constraint('ordered_sample_ibfk_2', 'ordered_sample', type_='foreignkey')
    op.create_foreign_key(None, 'ordered_sample', 'order', ['order_id'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.create_foreign_key(None, 'ordered_sample', 'ordered_sample', ['parent_sample_id'], ['id'], source_schema='nph',
                          referent_schema='nph')
    op.drop_constraint('study_category_ibfk_1', 'study_category', type_='foreignkey')
    op.create_foreign_key(None, 'study_category', 'study_category', ['parent_id'], ['id'], source_schema='nph',
                          referent_schema='nph')
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'study_category', schema='nph', type_='foreignkey')
    op.create_foreign_key('study_category_ibfk_1', 'study_category', 'study_category', ['parent_id'], ['id'])
    op.drop_constraint(None, 'ordered_sample', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'ordered_sample', schema='nph', type_='foreignkey')
    op.create_foreign_key('ordered_sample_ibfk_2', 'ordered_sample', 'order', ['order_id'], ['id'])
    op.create_foreign_key('ordered_sample_ibfk_1', 'ordered_sample', 'ordered_sample', ['parent_sample_id'], ['id'])
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.create_foreign_key('order_ibfk_3', 'order', 'site', ['finalized_site'], ['id'])
    op.create_foreign_key('order_ibfk_1', 'order', 'site', ['created_site'], ['id'])
    op.create_foreign_key('order_ibfk_4', 'order', 'site', ['collected_site'], ['id'])
    op.create_foreign_key('order_ibfk_6', 'order', 'study_category', ['category_id'], ['id'])
    op.create_foreign_key('order_ibfk_5', 'order', 'site', ['amended_site'], ['id'])
    op.create_foreign_key('order_ibfk_2', 'order', 'participant', ['participant_id'], ['id'])
    op.drop_table('pairing_event', schema='nph')
    op.drop_table('enrollment_event', schema='nph')
    op.drop_table('consent_event', schema='nph')
    op.drop_table('participant_event_activity', schema='nph')
    op.drop_table('pairing_event_type', schema='nph')
    op.drop_table('enrollment_event_type', schema='nph')
    op.drop_table('consent_event_type', schema='nph')
    op.drop_table('activity', schema='nph')
    # ### end Alembic commands ###
