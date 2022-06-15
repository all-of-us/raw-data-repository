"""Add ethnicity other columns to workbench_researcher

Revision ID: 622c84ab202a
Revises: 9d4ad0768cfd
Create Date: 2022-06-15 14:16:19.542797

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '622c84ab202a'
down_revision = '9d4ad0768cfd'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workbench_researcher', sa.Column('dsv2_ethnicity_black_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher', sa.Column('dsv2_ethnicity_hispanic_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher', sa.Column('dsv2_ethnicity_mena_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher', sa.Column('dsv2_ethnicity_nhpi_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher', sa.Column('dsv2_ethnicity_white_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('dsv2_ethnicity_black_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('dsv2_ethnicity_hispanic_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('dsv2_ethnicity_mena_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('dsv2_ethnicity_nhpi_other', sa.String(length=200), nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('dsv2_ethnicity_white_other', sa.String(length=200), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workbench_researcher_history', 'dsv2_ethnicity_white_other')
    op.drop_column('workbench_researcher_history', 'dsv2_ethnicity_nhpi_other')
    op.drop_column('workbench_researcher_history', 'dsv2_ethnicity_mena_other')
    op.drop_column('workbench_researcher_history', 'dsv2_ethnicity_hispanic_other')
    op.drop_column('workbench_researcher_history', 'dsv2_ethnicity_black_other')
    op.drop_column('workbench_researcher', 'dsv2_ethnicity_white_other')
    op.drop_column('workbench_researcher', 'dsv2_ethnicity_nhpi_other')
    op.drop_column('workbench_researcher', 'dsv2_ethnicity_mena_other')
    op.drop_column('workbench_researcher', 'dsv2_ethnicity_hispanic_other')
    op.drop_column('workbench_researcher', 'dsv2_ethnicity_black_other')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

