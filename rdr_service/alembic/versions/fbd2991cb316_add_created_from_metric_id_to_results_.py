"""add created_from_metric_id to results viewed and report states tables

Revision ID: fbd2991cb316
Revises: 2ea1a8e0acb0, 2d70a82af09b, 42428d88dd1d
Create Date: 2022-10-06 14:50:49.973035

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fbd2991cb316'
down_revision = ('2ea1a8e0acb0', '2d70a82af09b', '42428d88dd1d')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_member_report_state', sa.Column('created_from_metric_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_member_report_state', 'user_event_metrics', ['created_from_metric_id'], ['id'])
    op.add_column('genomic_result_viewed', sa.Column('created_from_metric_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_result_viewed', 'user_event_metrics', ['created_from_metric_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_result_viewed', type_='foreignkey')
    op.drop_column('genomic_result_viewed', 'created_from_metric_id')
    op.drop_constraint(None, 'genomic_member_report_state', type_='foreignkey')
    op.drop_column('genomic_member_report_state', 'created_from_metric_id')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
