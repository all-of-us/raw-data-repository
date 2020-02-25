"""new columns genomic set member history

Revision ID: 8f57152e65f4
Revises: 7ab9205d1bc6
Create Date: 2020-02-25 10:50:56.850438

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = '8f57152e65f4'
down_revision = '7ab9205d1bc6'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN consent_for_ror varchar(10);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN cvl_manifest_arr_job_run_id int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN cvl_manifest_wgs_job_run_id int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN reconcile_cvl_job_run_id int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN sequencing_file_name varchar(128);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN withdrawn_status int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN reconcile_gc_manifest_job_run_id int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN reconcile_metrics_bb_manifest_job_run_id int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN reconcile_metrics_sequencing_job_run_id int(11);")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN validation_flags varchar(80) AFTER validated_time")
    op.execute("ALTER TABLE genomic_set_member_history ADD COLUMN ai_an varchar(2);")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("genomic_set_member_history", "consent_for_ror")
    op.drop_column("genomic_set_member_history", "cvl_manifest_arr_job_run_id")
    op.drop_column("genomic_set_member_history", "cvl_manifest_wgs_job_run_id")
    op.drop_column("genomic_set_member_history", "reconcile_cvl_job_run_id")
    op.drop_column("genomic_set_member_history", "sequencing_file_name")
    op.drop_column("genomic_set_member_history", "withdrawn_status")
    op.drop_column("genomic_set_member_history", "reconcile_gc_manifest_job_run_id")
    op.drop_column("genomic_set_member_history", "reconcile_metrics_bb_manifest_job_run_id")
    op.drop_column("genomic_set_member_history", "reconcile_metrics_sequencing_job_run_id")
    op.drop_column("genomic_set_member_history", "validation_flags")
    op.drop_column("genomic_set_member_history", "ai_an")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

