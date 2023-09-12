"""change sample_id datatype

Revision ID: 639a51a6ea25
Revises: cf4839a3b84a
Create Date: 2023-09-12 15:47:00.055657

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '639a51a6ea25'
down_revision = 'cf4839a3b84a'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    op.execute('ALTER TABLE nph.sms_sample MODIFY COLUMN sample_id VARCHAR(32)')
    op.execute('ALTER TABLE nph.sms_n0 MODIFY COLUMN sample_id VARCHAR(32)')
    op.execute('ALTER TABLE nph.sms_n1_mc1 MODIFY COLUMN sample_id VARCHAR(32)')


def downgrade_nph():
    op.execute('ALTER TABLE nph.sms_sample MODIFY COLUMN sample_id BIGINT(20)')
    op.execute('ALTER TABLE nph.sms_n0 MODIFY COLUMN sample_id BIGINT(20)')
    op.execute('ALTER TABLE nph.sms_n1_mc1 MODIFY COLUMN sample_id BIGINT(20)')

