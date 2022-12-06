"""valuestring_to_text

Revision ID: e26ea978c345
Revises: 2be6f6d054e8
Create Date: 2018-04-03 12:49:01.059482

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e26ea978c345"
down_revision = "2be6f6d054e8"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "measurement", "value_string", existing_type=sa.String(1024), type_=sa.Text(), existing_nullable=True
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "measurement", "value_string", existing_type=sa.Text(), type_=sa.String(1024), existing_nullable=True
    )
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
