"""initialize enrollment_event_type

Revision ID: fa9281091ce8
Revises: b1685b23d87e
Create Date: 2023-02-01 15:35:38.743254

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'fa9281091ce8'
down_revision = 'b1685b23d87e'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    # Enrollment Event Types
    op.execute("""
                INSERT INTO nph.enrollment_event_type
                (created, modified, ignore_flag, name)
                VALUES
                (now(), now(), 0, "Module 1 Consented"),
                (now(), now(), 0, "Module 1 Eligibility Confirmed"),
                (now(), now(), 0, "Module 1 Eligibility Failed"),
                (now(), now(), 0, "Module 1 Started"),
                (now(), now(), 0, "Module 1 Complete"),
                (now(), now(), 0, "Module 2 Consented"),
                (now(), now(), 0, "Module 2 Eligibility Confirmed"),
                (now(), now(), 0, "Module 2 Eligibility Failed"),
                (now(), now(), 0, "Module 2 Started"),
                (now(), now(), 0, "Module 2 Diet Assigned"),
                (now(), now(), 0, "Module 2 Complete"),
                (now(), now(), 0, "Module 3 Consented"),
                (now(), now(), 0, "Module 3 Eligibility Confirmed"),
                (now(), now(), 0, "Module 3 Eligibility Failed"),
                (now(), now(), 0, "Module 3 Started"),
                (now(), now(), 0, "Module 3 Diet Assigned"),
                (now(), now(), 0, "Module 3 Complete"),
                (now(), now(), 0, "Withdrawn"),
                (now(), now(), 0, "Deactivated")
            """)


def downgrade_nph():
    op.execute("""DELETE FROM nph.enrollent_event_type WHERE name IN ()""")
