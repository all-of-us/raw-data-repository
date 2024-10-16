"""adding_event_models

Revision ID: d69602b0fbdc
Revises: fa1c82efc935
Create Date: 2024-05-09 20:56:58.299799

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'd69602b0fbdc'
down_revision = 'fa1c82efc935'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_ppsc():
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
    schema='ppsc'
    )
    op.create_table('enrollment_event_type',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('source_name', sa.String(length=128), nullable=True),
    sa.Column('rule_codes', mysql.JSON(), nullable=True),
    sa.Column('version', sa.String(length=128), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_table('participant_event_activity',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('activity_id', sa.Integer(), nullable=True),
    sa.Column('resource', mysql.JSON(), nullable=True),
    sa.ForeignKeyConstraint(['activity_id'], ['ppsc.activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['ppsc.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_table('enrollment_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_id', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['ppsc.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['event_type_id'], ['ppsc.enrollment_event_type.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['ppsc.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.add_column('participant', sa.Column('registered_date', rdr_service.model.utils.UTCDateTime(), nullable=False))
    op.drop_index('research_id', table_name='participant')
    op.drop_column('participant', 'research_id')

    op.execute(
        """
        INSERT INTO ppsc.activity (created, modified, name, ignore_flag)
        VALUES (now(), now(), 'ENROLLMENT', 0);
        """
    )

    op.execute(
        """
        INSERT INTO ppsc.enrollment_event_type (created, modified, ignore_flag, name, source_name)
        VALUES (now(), now(), 0, 'Participant Created', 'participant_created');
        """
    )

    # ### end Alembic commands ###


def downgrade_ppsc():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant', sa.Column('research_id', mysql.BIGINT(display_width=20), autoincrement=False,
                                           nullable=True))
    op.create_index('research_id', 'participant', ['research_id'], unique=True)
    op.drop_column('participant', 'registered_date')
    op.drop_table('enrollment_event', schema='ppsc')
    op.drop_table('participant_event_activity', schema='ppsc')
    op.drop_table('enrollment_event_type', schema='ppsc')
    op.drop_table('activity', schema='ppsc')
    # ### end Alembic commands ###

