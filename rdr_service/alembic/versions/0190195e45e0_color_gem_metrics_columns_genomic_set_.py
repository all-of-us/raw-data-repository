"""color_gem_metrics_columns_genomic_set_member

Revision ID: 0190195e45e0
Revises: 2c3a71f9fc04
Create Date: 2020-08-28 15:58:18.193945

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0190195e45e0'
down_revision = '2c3a71f9fc04'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_set_member', sa.Column('color_metrics_job_run_id', sa.Integer(), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('color_metrics_job_run_id',
                                                          sa.Integer(), nullable=True))

    op.add_column('genomic_set_member', sa.Column('gem_metrics_ancestry_loop_response',
                                                  sa.String(length=10), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('gem_metrics_ancestry_loop_response',
                                                  sa.String(length=10), nullable=True))

    op.add_column('genomic_set_member', sa.Column('gem_metrics_available_results',
                                                  sa.String(length=255), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('gem_metrics_available_results',
                                                          sa.String(length=255), nullable=True))

    op.add_column('genomic_set_member', sa.Column('gem_metrics_results_released_at', sa.DateTime(), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('gem_metrics_results_released_at',
                                                          sa.DateTime(), nullable=True))

    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['color_metrics_job_run_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')

    op.drop_column('genomic_set_member', 'gem_metrics_results_released_at')
    op.drop_column('genomic_set_member_history', 'gem_metrics_results_released_at')

    op.drop_column('genomic_set_member', 'gem_metrics_available_results')
    op.drop_column('genomic_set_member_history', 'gem_metrics_available_results')

    op.drop_column('genomic_set_member', 'gem_metrics_ancestry_loop_response')
    op.drop_column('genomic_set_member_history', 'gem_metrics_ancestry_loop_response')

    op.drop_column('genomic_set_member', 'color_metrics_job_run_id')
    op.drop_column('genomic_set_member_history', 'color_metrics_job_run_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
