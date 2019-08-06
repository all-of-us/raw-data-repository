"""alter MetricsAgeCache add type column

Revision ID: 014053444333
Revises: 5aa0142f9bcb
Create Date: 2019-02-25 15:05:13.285031

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '014053444333'
down_revision = '5aa0142f9bcb'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE metrics_age_cache ADD COLUMN type varchar(50) NOT NULL default \'METRICS_V2_API\' AFTER `date_inserted`;')
    op.execute('ALTER TABLE metrics_age_cache DROP PRIMARY KEY')
    op.execute('ALTER TABLE metrics_age_cache ADD PRIMARY KEY (`date_inserted`,`type`,`hpo_id`,`hpo_name`,`date`,`age_range`)')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('metrics_age_cache', 'type')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

