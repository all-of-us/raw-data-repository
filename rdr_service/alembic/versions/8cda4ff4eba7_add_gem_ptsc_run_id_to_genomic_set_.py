"""add gem ptsc run id to genomic_set_member

Revision ID: 8cda4ff4eba7
Revises: 67710db9e2e1
Create Date: 2020-03-11 08:49:20.970167

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '8cda4ff4eba7'
down_revision = '67710db9e2e1'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_set_member', sa.Column('gem_ptsc_sent_job_run_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['gem_ptsc_sent_job_run_id'], ['id'])
    op.add_column('genomic_set_member_history', sa.Column('gem_ptsc_sent_job_run_id', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.drop_column('genomic_set_member', 'gem_ptsc_sent_job_run_id')
    op.drop_column('genomic_set_member_history', 'gem_ptsc_sent_job_run_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
