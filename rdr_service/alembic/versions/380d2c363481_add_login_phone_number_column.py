"""add login_phone_number column

Revision ID: 380d2c363481
Revises: e6605d4b0dba
Create Date: 2018-10-03 11:42:59.993435

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "380d2c363481"
down_revision = "e6605d4b0dba"
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
    op.add_column("participant_summary", sa.Column("login_phone_number", sa.String(length=80), nullable=True))
    op.alter_column("participant_summary", "email", existing_type=mysql.VARCHAR(length=255), nullable=True)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("participant_summary", "email", existing_type=mysql.VARCHAR(length=255), nullable=False)
    op.drop_column("participant_summary", "login_phone_number")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
