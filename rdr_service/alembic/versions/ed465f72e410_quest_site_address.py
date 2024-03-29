"""quest site address

Revision ID: ed465f72e410
Revises: 94fa5d266916
Create Date: 2020-06-18 13:07:43.826284

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'ed465f72e410'
down_revision = '94fa5d266916'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('biobank_quest_order_site_address',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('address1', sa.String(length=500), nullable=True),
    sa.Column('address2', sa.String(length=500), nullable=True),
    sa.Column('city', sa.String(length=80), nullable=True),
    sa.Column('state', sa.String(length=50), nullable=True),
    sa.Column('zip_code', sa.String(length=50), nullable=True),
    sa.Column('biobank_order_id', sa.String(length=80), nullable=False),
    sa.ForeignKeyConstraint(['biobank_order_id'], ['biobank_order.biobank_order_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('biobank_quest_order_site_address')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
