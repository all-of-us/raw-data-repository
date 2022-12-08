"""adding_user_events_model

Revision ID: e2eced303a1f
Revises: 42a07448cc24, 263cbd12ce25
Create Date: 2021-12-17 08:28:48.620429

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e2eced303a1f'
down_revision = ('42a07448cc24', '263cbd12ce25')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('user_event_metrics',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.String(length=255), nullable=True),
    sa.Column('event_name', sa.String(length=512), nullable=True),
    sa.Column('device', sa.String(length=255), nullable=True),
    sa.Column('operating_system', sa.String(length=255), nullable=True),
    sa.Column('browser', sa.String(length=255), nullable=True),
    sa.Column('file_path', sa.String(length=512), nullable=True),
    sa.Column('run_id', sa.Integer(), nullable=False),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['genomic_job_run.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_user_event_metrics_file_path'), 'user_event_metrics', ['file_path'], unique=False)
    op.create_index(op.f('ix_user_event_metrics_participant_id'), 'user_event_metrics', ['participant_id'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_user_event_metrics_participant_id'), table_name='user_event_metrics')
    op.drop_index(op.f('ix_user_event_metrics_file_path'), table_name='user_event_metrics')
    op.drop_table('user_event_metrics')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
