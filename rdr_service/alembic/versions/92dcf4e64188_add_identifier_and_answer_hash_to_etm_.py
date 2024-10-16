"""Add identifier and response_hash to etm_questionnaire_response

Revision ID: 92dcf4e64188
Revises: 6b8cc85c5389
Create Date: 2024-05-08 11:08:02.291276

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '92dcf4e64188'
down_revision = '6b8cc85c5389'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('etm_questionnaire_response', sa.Column('response_hash', sa.String(length=32), nullable=True))
    op.add_column('etm_questionnaire_response', sa.Column('identifier', sa.String(length=64), nullable=True))
    op.execute("""UPDATE etm_questionnaire_response
                    SET identifier = json_unquote(json_extract(resource, '$.identifier.value'))
                  WHERE identifier IS NULL""")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('etm_questionnaire_response', 'identifier')
    op.drop_column('etm_questionnaire_response', 'response_hash')
    # ### end Alembic commands ###

def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

