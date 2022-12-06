"""add obsolete columns

Revision ID: 4a9ceee64d0b
Revises: 69453413dfc3
Create Date: 2018-07-13 09:52:49.048544

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.model.site_enums import ObsoleteStatus

# revision identifiers, used by Alembic.
revision = "4a9ceee64d0b"
down_revision = "69453413dfc3"
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
    op.add_column("hpo", sa.Column("is_obsolete", model.utils.Enum(ObsoleteStatus), nullable=True))
    op.add_column("organization", sa.Column("is_obsolete", model.utils.Enum(ObsoleteStatus), nullable=True))
    op.add_column("site", sa.Column("is_obsolete", model.utils.Enum(ObsoleteStatus), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("site", "is_obsolete")
    op.drop_column("organization", "is_obsolete")
    op.drop_column("hpo", "is_obsolete")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
