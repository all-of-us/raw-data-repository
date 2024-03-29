"""Added Incident model

Revision ID: c595aaa99385
Revises: 7c2bea760b6e, eee3c3d4e4d4
Create Date: 2023-02-28 10:21:11.082308

"""


# pylint: disable=unused-import
# pylint: disable=line-too-long
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

from rdr_service.ancillary_study_resources.nph.enums import IncidentStatus, IncidentType

# revision identifiers, used by Alembic.
revision = 'c595aaa99385'
down_revision = ('7c2bea760b6e', 'eee3c3d4e4d4')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('incident',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('dev_note', sa.String(length=1024), nullable=True),
    sa.Column('status_str', sa.String(length=512), nullable=True),
    sa.Column('status_id', rdr_service.model.utils.Enum(IncidentStatus), nullable=True),
    sa.Column('message', sa.String(length=1024), nullable=True),
    sa.Column('notification_sent_flag', mysql.TINYINT(), nullable=True),
    sa.Column('notification_date', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('incident_type_str', sa.String(length=512), nullable=True),
    sa.Column('incident_type_id', rdr_service.model.utils.Enum(IncidentType), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('trace_id', sa.String(length=128), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['nph.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('incident', schema='nph')
    # ### end Alembic commands ###
