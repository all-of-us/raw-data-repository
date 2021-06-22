"""add access tier to workspace

Revision ID: fc1eb86aa8f4
Revises: a2d41894a561
Create Date: 2021-06-22 13:26:42.138628

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import WorkbenchWorkspaceAccessTier

# revision identifiers, used by Alembic.
revision = 'fc1eb86aa8f4'
down_revision = 'a2d41894a561'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workbench_workspace_approved', sa.Column('access_tier', rdr_service.model.utils.Enum(WorkbenchWorkspaceAccessTier), nullable=True))
    op.add_column('workbench_workspace_snapshot', sa.Column('access_tier', rdr_service.model.utils.Enum(WorkbenchWorkspaceAccessTier), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workbench_workspace_snapshot', 'access_tier')
    op.drop_column('workbench_workspace_approved', 'access_tier')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

