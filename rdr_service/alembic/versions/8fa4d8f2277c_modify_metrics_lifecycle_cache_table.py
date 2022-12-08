"""modify metrics_lifecycle_cache table

Revision ID: 8fa4d8f2277c
Revises: 6f042370e83b
Create Date: 2019-07-18 11:36:23.949486

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "8fa4d8f2277c"
down_revision = "6f042370e83b"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    op.execute("ALTER TABLE metrics_lifecycle_cache ADD COLUMN type varchar(50) NOT NULL AFTER `date_inserted`;")
    op.execute(
        "ALTER TABLE metrics_lifecycle_cache ADD COLUMN retention_modules_eligible int(11) NOT NULL AFTER `ppi_baseline_complete`;"
    )
    op.execute(
        "ALTER TABLE metrics_lifecycle_cache ADD COLUMN retention_modules_complete int(11) NOT NULL AFTER `retention_modules_eligible`;"
    )
    op.execute("ALTER TABLE metrics_lifecycle_cache DROP PRIMARY KEY")
    op.execute(
        "ALTER TABLE metrics_lifecycle_cache ADD PRIMARY KEY (`date_inserted`,`type`,`hpo_id`,`hpo_name`,`date`)"
    )


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE metrics_lifecycle_cache DROP PRIMARY KEY")
    op.drop_column("metrics_lifecycle_cache", "type")
    op.drop_column("metrics_lifecycle_cache", "retention_modules_eligible")
    op.drop_column("metrics_lifecycle_cache", "retention_modules_complete")
    op.execute("ALTER TABLE metrics_lifecycle_cache ADD PRIMARY KEY (`date_inserted`,`hpo_id`,`hpo_name`,`date`)")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
