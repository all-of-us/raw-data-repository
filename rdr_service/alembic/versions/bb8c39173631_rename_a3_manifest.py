"""rename a3 manifest

Revision ID: bb8c39173631
Revises: 98595efd597e
Create Date: 2020-04-07 08:44:55.893170

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'bb8c39173631'
down_revision = '98595efd597e'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_set_member', sa.Column('gem_a3_manifest_job_run_id', sa.Integer(), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('gem_a3_manifest_job_run_id', sa.Integer(), nullable=True))
    op.drop_constraint('genomic_set_member_ibfk_15', 'genomic_set_member', type_='foreignkey')
    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['gem_a3_manifest_job_run_id'], ['id'])
    op.drop_column('genomic_set_member', 'gem_a2d_manifest_job_run_id')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_set_member', sa.Column('gem_a2d_manifest_job_run_id', mysql.INTEGER(
        display_width=11), autoincrement=False, nullable=True))
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.create_foreign_key('genomic_set_member_ibfk_15', 'genomic_set_member', 'genomic_job_run', ['gem_a2d_manifest_job_run_id'], ['id'])
    op.drop_column('genomic_set_member', 'gem_a3_manifest_job_run_id')
    op.drop_column('genomic_set_member_history', 'gem_a3_manifest_job_run_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
