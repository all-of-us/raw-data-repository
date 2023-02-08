"""allowing for string on sum-check

Revision ID: 9d100aca2518
Revises: 1cdbe0f0608e, d3461d54e75d
Create Date: 2023-02-07 14:32:12.187401

"""
from alembic import op
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = '9d100aca2518'
down_revision = ('1cdbe0f0608e', 'd3461d54e75d')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    op.alter_column(
        'biobank_file_export',
        'crc32c_checksum',
        type_=mysql.VARCHAR(length=64),
        existing_type=mysql.INTEGER(display_width=11)
    )


def downgrade_nph():
    op.alter_column(
        'biobank_file_export',
        'crc32c_checksum',
        type_=mysql.INTEGER(display_width=11),
        existing_type=mysql.VARCHAR(length=64)
    )
