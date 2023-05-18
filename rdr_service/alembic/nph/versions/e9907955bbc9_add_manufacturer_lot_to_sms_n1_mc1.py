"""add manufacturer_lot to sms_n1_mc1

Revision ID: e9907955bbc9
Revises: 4869da6593e8, c9af481383d9, 6709642eb172
Create Date: 2023-05-09 13:12:22.984772

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'e9907955bbc9'
down_revision = ('4869da6593e8', 'c9af481383d9', '6709642eb172')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("""
        ALTER TABLE nph.sms_n1_mc1 ADD COLUMN manufacturer_lot varchar(32) AFTER tracking_number
    """)
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('sms_n1_mc1', 'manufacturer_lot')
    # ### end Alembic commands ###
