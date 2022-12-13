"""add education and disability for workbench research

Revision ID: f4316bf05797
Revises: 038364a84126
Create Date: 2020-01-09 14:48:57.556654

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import WorkbenchResearcherEducation, WorkbenchResearcherDisability

# revision identifiers, used by Alembic.
revision = 'f4316bf05797'
down_revision = '038364a84126'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workbench_researcher', sa.Column('disability',
                                                    rdr_service.model.utils.Enum(WorkbenchResearcherDisability),
                                                    nullable=True))
    op.add_column('workbench_researcher', sa.Column('education',
                                                    rdr_service.model.utils.Enum(WorkbenchResearcherEducation),
                                                    nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('disability',
                                                            rdr_service.model.utils.Enum(WorkbenchResearcherDisability),
                                                            nullable=True))
    op.add_column('workbench_researcher_history', sa.Column('education',
                                                            rdr_service.model.utils.Enum(WorkbenchResearcherEducation),
                                                            nullable=True))
    op.execute("ALTER TABLE workbench_researcher CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher CHANGE COLUMN `modified` `modified` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher_history CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher_history CHANGE COLUMN `modified` `modified` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher CHANGE COLUMN `creation_time` `creation_time` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher CHANGE COLUMN `modified_time` `modified_time` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher_history CHANGE COLUMN `creation_time` `creation_time` DATETIME(6);")
    op.execute("ALTER TABLE workbench_researcher_history CHANGE COLUMN `modified_time` `modified_time` DATETIME(6);")

    op.execute("ALTER TABLE workbench_institutional_affiliations CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_institutional_affiliations CHANGE COLUMN `modified` `modified` DATETIME(6);")
    op.execute("ALTER TABLE workbench_institutional_affiliations_history CHANGE COLUMN `created` `created` "
               "DATETIME(6);")
    op.execute("ALTER TABLE workbench_institutional_affiliations_history CHANGE COLUMN `modified` `modified` "
               "DATETIME(6);")

    op.execute("ALTER TABLE workbench_workspace CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace CHANGE COLUMN `modified` `modified` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_history CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_history CHANGE COLUMN `modified` `modified` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace CHANGE COLUMN `creation_time` `creation_time` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace CHANGE COLUMN `modified_time` `modified_time` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_history CHANGE COLUMN `creation_time` `creation_time` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_history CHANGE COLUMN `modified_time` `modified_time` DATETIME(6);")

    op.execute("ALTER TABLE workbench_workspace_user CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_user CHANGE COLUMN `modified` `modified` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_user_history CHANGE COLUMN `created` `created` DATETIME(6);")
    op.execute("ALTER TABLE workbench_workspace_user_history CHANGE COLUMN `modified` `modified` DATETIME(6);")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workbench_researcher_history', 'education')
    op.drop_column('workbench_researcher_history', 'disability')
    op.drop_column('workbench_researcher', 'education')
    op.drop_column('workbench_researcher', 'disability')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
