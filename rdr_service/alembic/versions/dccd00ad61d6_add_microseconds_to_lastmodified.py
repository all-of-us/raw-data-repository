"""Add microseconds to lastModified

Revision ID: dccd00ad61d6
Revises: fac398dd2426
Create Date: 2019-02-15 10:27:19.746064

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "dccd00ad61d6"
down_revision = "fac398dd2426"
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
    op.execute("ALTER TABLE participant CHANGE COLUMN `last_modified` `last_modified` DATETIME(6) NULL")
    op.execute("ALTER TABLE participant_summary CHANGE COLUMN `last_modified` `last_modified` DATETIME(6) NULL")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE participant CHANGE COLUMN `last_modified` `last_modified` DATETIME(0) NULL")
    op.execute("ALTER TABLE participant_summary CHANGE COLUMN `last_modified` `last_modified` DATETIME(0) NULL")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
