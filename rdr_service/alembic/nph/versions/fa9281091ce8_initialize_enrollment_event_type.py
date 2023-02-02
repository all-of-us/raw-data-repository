"""initialize enrollment_event_type

Revision ID: fa9281091ce8
Revises: b1685b23d87e
Create Date: 2023-02-01 15:35:38.743254

"""
from alembic import op
import sqlalchemy as sa

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
    op.add_column('consent_event_type', sa.Column('source_name', sa.String(length=128), nullable=True))
    op.add_column('enrollment_event_type', sa.Column('source_name', sa.String(length=128), nullable=True))
    op.add_column('pairing_event_type', sa.Column('source_name', sa.String(length=128), nullable=True))
    # Enrollment Event Types
    op.execute("""
                INSERT INTO nph.enrollment_event_type
                (created, modified, ignore_flag, name, source_name)
                VALUES
                (now(), now(), 0, "Module 1 Consented", null),
                (now(), now(), 0, "Module 1 Eligibility Confirmed", "eligibilityConfirmed"),
                (now(), now(), 0, "Module 1 Eligibility Failed", null),
                (now(), now(), 0, "Module 1 Started", null),
                (now(), now(), 0, "Module 1 Complete", null),
                (now(), now(), 0, "Module 2 Consented", null),
                (now(), now(), 0, "Module 2 Eligibility Confirmed", null),
                (now(), now(), 0, "Module 2 Eligibility Failed", null),
                (now(), now(), 0, "Module 2 Started", null),
                (now(), now(), 0, "Module 2 Diet Assigned", null),
                (now(), now(), 0, "Module 2 Complete", null),
                (now(), now(), 0, "Module 3 Consented", null),
                (now(), now(), 0, "Module 3 Eligibility Confirmed", null),
                (now(), now(), 0, "Module 3 Eligibility Failed", null),
                (now(), now(), 0, "Module 3 Started", null),
                (now(), now(), 0, "Module 3 Diet Assigned", null),
                (now(), now(), 0, "Module 3 Complete", null),
                (now(), now(), 0, "Withdrawn", null),
                (now(), now(), 0, "Deactivated", null)
            """)


def downgrade_nph():
    op.drop_column('pairing_event_type', 'source_name')
    op.drop_column('enrollment_event_type', 'source_name')
    op.drop_column('consent_event_type', 'source_name')
    op.execute("""DELETE FROM nph.enrollent_event_type WHERE name IN (
                "Module 1 Consented",
                "Module 1 Eligibility Confirmed",
                "Module 1 Eligibility Failed",
                "Module 1 Started",
                "Module 1 Complete",
                "Module 2 Consented",
                "Module 2 Eligibility Confirmed",
                "Module 2 Eligibility Failed",
                "Module 2 Started",
                "Module 2 Diet Assigned",
                "Module 2 Complete",
                "Module 3 Consented",
                "Module 3 Eligibility Confirmed",
                "Module 3 Eligibility Failed",
                "Module 3 Started",
                "Module 3 Diet Assigned",
                "Module 3 Complete",
                "Withdrawn",
                "Deactivated")""")
