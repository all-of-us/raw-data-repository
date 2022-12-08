"""migration_gem_report_authored_time

Revision ID: bbf2f8d73a96
Revises: 82f9b9a31acd
Create Date: 2022-08-30 15:54:34.210582

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'bbf2f8d73a96'
down_revision = '82f9b9a31acd'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        """
        UPDATE genomic_member_report_state gmsp
            INNER JOIN genomic_set_member gsm
            On gsm.id = gmsp.genomic_set_member_id
            INNER JOIN genomic_job_run gjr
            On gjr.id = gsm.gem_a2_manifest_job_run_id
        SET gmsp.event_authored_time = gjr.created,
            gmsp.event_type = 'result_ready',
            gmsp.sample_id = gsm.sample_id,
            gmsp.modified = NOW()
        Where gsm.gem_a2_manifest_job_run_id is not null
          And gsm.genome_type = 'aou_array'
          And gmsp.module = 'gem'
        """
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
