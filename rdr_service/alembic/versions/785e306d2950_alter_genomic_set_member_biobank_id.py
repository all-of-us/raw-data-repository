"""alter_genomic_set_member_biobank_id

Revision ID: 785e306d2950
Revises: 66765ee98a07
Create Date: 2021-01-28 12:24:37.988895

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '785e306d2950'
down_revision = '7e9c89411010'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE genomic_set_member MODIFY biobank_id varchar(128) null')
    op.execute('ALTER TABLE genomic_set_member_history MODIFY biobank_id varchar(128) null')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE genomic_set_member MODIFY biobank_id int(11) null')
    op.execute('ALTER TABLE genomic_set_member_history MODIFY biobank_id int(11) null')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

