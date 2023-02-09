"""removing unique contraint on check sum

Revision ID: 7c2bea760b6e
Revises: 9d100aca2518
Create Date: 2023-02-09 10:24:59.119481

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '7c2bea760b6e'
down_revision = '9d100aca2518'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    op.drop_index(op.f('crc32c_checksum'), table_name='biobank_file_export', schema='nph')


def downgrade_nph():
    op.create_index('crc32c_checksum', 'biobank_file_export', ['crc32c_checksum'], unique=True)
