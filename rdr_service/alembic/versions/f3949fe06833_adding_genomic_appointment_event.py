"""adding_genomic_appointment_event

Revision ID: f3949fe06833
Revises: 8cff129d4c39
Create Date: 2022-07-26 13:42:30.698692

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils

# revision identifiers, used by Alembic.
revision = 'f3949fe06833'
down_revision = '8cff129d4c39'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_appointment_event',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('message_record_id', sa.Integer(), nullable=True),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('event_type', sa.String(length=256), nullable=False),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('module_type', sa.String(length=255), nullable=True),
    sa.Column('appointment_id', sa.Integer(), nullable=False),
    sa.Column('appointment_time', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('source', sa.String(length=255), nullable=True),
    sa.Column('location', sa.String(length=255), nullable=True),
    sa.Column('contact_number', sa.String(length=255), nullable=True),
    sa.Column('language', sa.String(length=255), nullable=True),
    sa.Column('cancellation_reason', sa.String(length=255), nullable=True),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('genomic_appointment_event')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
