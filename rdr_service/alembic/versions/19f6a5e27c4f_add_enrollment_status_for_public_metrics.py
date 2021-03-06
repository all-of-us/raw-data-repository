"""add enrollment status for public metrics

Revision ID: 19f6a5e27c4f
Revises: b82e99329bf5
Create Date: 2019-04-25 11:26:15.309679

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "19f6a5e27c4f"
down_revision = "b82e99329bf5"
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE metrics_race_cache ADD COLUMN registered_flag BOOLEAN NOT NULL AFTER `date_inserted`;")
    op.execute("ALTER TABLE metrics_race_cache ADD COLUMN consent_flag BOOLEAN NOT NULL AFTER `registered_flag`;")
    op.execute("ALTER TABLE metrics_race_cache ADD COLUMN core_flag BOOLEAN NOT NULL AFTER `consent_flag`;")
    op.execute("ALTER TABLE metrics_race_cache DROP PRIMARY KEY")
    op.execute(
        "ALTER TABLE metrics_race_cache ADD PRIMARY KEY (`date_inserted`,`registered_flag`,`consent_flag`,`core_flag`,`hpo_id`,`hpo_name`,`date`)"
    )

    op.execute(
        "ALTER TABLE metrics_gender_cache ADD COLUMN enrollment_status VARCHAR(50) NOT NULL AFTER `date_inserted`;"
    )
    op.execute("ALTER TABLE metrics_gender_cache DROP PRIMARY KEY")
    op.execute(
        "ALTER TABLE metrics_gender_cache ADD PRIMARY KEY (`date_inserted`,`hpo_id`,`hpo_name`,`date`,`gender_name`,`enrollment_status`)"
    )

    op.execute(
        "ALTER TABLE metrics_age_cache ADD COLUMN enrollment_status VARCHAR(50) NOT NULL AFTER `date_inserted`;"
    )
    op.execute("ALTER TABLE metrics_age_cache DROP PRIMARY KEY")
    op.execute(
        "ALTER TABLE metrics_age_cache ADD PRIMARY KEY (`date_inserted`,`type`,`hpo_id`,`hpo_name`,`date`,`age_range`,`enrollment_status`)"
    )

    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("metrics_race_cache", "registered_flag")
    op.drop_column("metrics_race_cache", "core_flag")
    op.drop_column("metrics_race_cache", "consent_flag")
    op.drop_column("metrics_gender_cache", "enrollment_status")
    op.drop_column("metrics_age_cache", "enrollment_status")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
