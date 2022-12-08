"""add columns to genomic_set_member_history table

Revision ID: d1f67196215e
Revises: f512f8ca07c2
Create Date: 2019-05-22 12:00:00.262058

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "d1f67196215e"
down_revision = "f512f8ca07c2"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN biobank_id varchar(80) AFTER `biobank_order_id`;")
    op.execute(
        "ALTER TABLE genomic_set_member_history ADD COLUMN biobank_order_client_Id varchar(80) AFTER `biobank_id`;"
    )
    op.execute(
        "ALTER TABLE genomic_set_member_history ADD COLUMN package_id varchar(80) AFTER `biobank_order_client_Id`;"
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("genomic_set_member_history", "package_id")
    op.drop_column("genomic_set_member_history", "biobank_order_client_Id")
    op.drop_column("genomic_set_member_history", "biobank_id")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
