"""add is_reviewed to workspace table

Revision ID: 679d13d850ce
Revises: 1edcfe7d61ec
Create Date: 2020-04-29 12:53:17.135885

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '679d13d850ce'
down_revision = '1edcfe7d61ec'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workbench_workspace_approved', sa.Column('is_reviewed', sa.Boolean(), nullable=True))
    op.add_column('workbench_workspace_snapshot', sa.Column('is_reviewed', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workbench_workspace_snapshot', 'is_reviewed')
    op.drop_column('workbench_workspace_approved', 'is_reviewed')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
