"""alter_genomic_set_member_string_fields_length

Revision ID: d772959915d5
Revises: 785e306d2950
Create Date: 2021-02-10 12:16:09.781012

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = 'd772959915d5'
down_revision = '785e306d2950'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN sequencing_file_name varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_box_storage_unit_id varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_box_plate_id varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_study varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_tracking_number varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_contact varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_email varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_study_pi varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_test_name varchar(255) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_failure_description varchar(255) null')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN sequencing_file_name varchar(128) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_box_storage_unit_id varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_box_plate_id varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_study varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_tracking_number varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_contact varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_email varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_study_pi varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_test_name varchar(50)) null')
    op.execute('ALTER TABLE genomic_set_member MODIFY COLUMN gc_manifest_failure_description varchar(128) null')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
