"""adding_new_nph_events

Revision ID: 9e8594edc75b
Revises: 9d100aca2518
Create Date: 2023-02-09 12:54:46.812332

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '9e8594edc75b'
down_revision = '9d100aca2518'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('deactivated_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['nph.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.create_table('withdrawal_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['nph.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('withdrawal_event', schema='nph')
    op.drop_table('deactivated_event', schema='nph')
    # ### end Alembic commands ###

