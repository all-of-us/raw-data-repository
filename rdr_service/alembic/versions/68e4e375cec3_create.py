"""Create

Revision ID: 68e4e375cec3
Revises: 40b97a574bec
Create Date: 2022-10-18 15:15:24.549856

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '68e4e375cec3'
down_revision = '40b97a574bec'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_appointment_event_notified',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('appointment_event_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['appointment_event_id'], ['genomic_appointment_event.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_appointment_event_notified_appointment_event_id'), 'genomic_appointment_event_notified', ['appointment_event_id'], unique=False)
    op.create_index(op.f('ix_genomic_appointment_event_notified_participant_id'), 'genomic_appointment_event_notified', ['participant_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_genomic_appointment_event_notified_participant_id'), table_name='genomic_appointment_event_notified')
    op.drop_index(op.f('ix_genomic_appointment_event_notified_appointment_event_id'), table_name='genomic_appointment_event_notified')
    op.drop_table('genomic_appointment_event_notified')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
