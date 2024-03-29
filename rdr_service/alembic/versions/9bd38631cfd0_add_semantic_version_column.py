"""add_semantic_version_column

Revision ID: 9bd38631cfd0
Revises: 844fd8dadd9f
Create Date: 2019-12-10 11:06:40.119572

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '9bd38631cfd0'
down_revision = '844fd8dadd9f'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE questionnaire ADD COLUMN semantic_version varchar(100) AFTER `version`;')
    op.execute('ALTER TABLE questionnaire_history ADD COLUMN semantic_version varchar(100) AFTER `version`;')
    op.execute('ALTER TABLE questionnaire_response ADD COLUMN questionnaire_semantic_version varchar(100) '
               'AFTER `questionnaire_version`;')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('questionnaire_response', 'questionnaire_semantic_version')
    op.drop_column('questionnaire_history', 'semantic_version')
    op.drop_column('questionnaire', 'semantic_version')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
