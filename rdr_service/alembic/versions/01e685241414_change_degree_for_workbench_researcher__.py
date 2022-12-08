"""change_degree_for_workbench_researcher__model

Revision ID: 01e685241414
Revises: caf125e99d1a
Create Date: 2020-02-06 13:57:51.552826

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '01e685241414'
down_revision = 'caf125e99d1a'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE `workbench_researcher` MODIFY `degree` JSON;")
    op.execute("ALTER TABLE `workbench_researcher_history` MODIFY `degree` JSON;")
    op.execute("ALTER TABLE `workbench_researcher` MODIFY `ethnicity` smallint(6);")
    op.execute("ALTER TABLE `workbench_researcher_history` MODIFY `ethnicity` smallint(6);")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE `workbench_researcher` MODIFY `degree` smallint(6);")
    op.execute("ALTER TABLE `workbench_researcher_history` MODIFY `degree` smallint(6);")
    op.execute("ALTER TABLE `workbench_researcher` MODIFY `ethnicity` JSON;")
    op.execute("ALTER TABLE `workbench_researcher_history` MODIFY `ethnicity` JSON;")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
