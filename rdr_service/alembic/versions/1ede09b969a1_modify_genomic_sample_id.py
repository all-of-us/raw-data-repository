"""modify_genomic_sample_id

Revision ID: 1ede09b969a1
Revises: e968d868a097
Create Date: 2020-02-05 10:14:04.546655

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from sqlalchemy.dialects import mysql

from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = '1ede09b969a1'
down_revision = 'e968d868a097'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('genomic_gc_validation_metrics', 'sample_id',
               existing_type=mysql.VARCHAR(length=80),
               nullable=True)
    op.create_foreign_key(None, 'genomic_gc_validation_metrics', 'biobank_stored_sample', ['sample_id'], ['biobank_stored_sample_id'])
    op.alter_column('genomic_set_member', 'sample_id',
               existing_type=mysql.VARCHAR(length=80),
               nullable=True)
    op.create_foreign_key(None, 'genomic_set_member', 'biobank_stored_sample', ['sample_id'], ['biobank_stored_sample_id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.alter_column('genomic_set_member', 'sample_id',
               existing_type=mysql.VARCHAR(length=80),
               nullable=True)
    op.drop_constraint(None, 'genomic_gc_validation_metrics', type_='foreignkey')
    op.alter_column('genomic_gc_validation_metrics', 'sample_id',
               existing_type=mysql.VARCHAR(length=80),
               nullable=True)
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

