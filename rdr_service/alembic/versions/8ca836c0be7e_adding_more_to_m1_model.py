"""adding_more_to_m1_model

Revision ID: 8ca836c0be7e
Revises: d7321a399c60
Create Date: 2024-10-17 15:04:08.993184

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
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
revision = '8ca836c0be7e'
down_revision = 'd7321a399c60'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('exposomics_m1', sa.Column('biobankid_sampleid', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('box_id_plate_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('box_storageunit_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('collection_date', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('collection_tube_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('contact', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('email', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('matrix_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('ny_flag', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('package_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('parent_sample_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('quantity_ul', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('rqs', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('sample_id', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('sample_type', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('study_name', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('total_concentration_ng_ul', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('total_yield_ng', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('tracking_number', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('treatment_type', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('two_sixty_two_eighty', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('two_sixty_two_thirty', sa.String(length=255), nullable=True))
    op.add_column('exposomics_m1', sa.Column('well_position', sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('exposomics_m1', 'well_position')
    op.drop_column('exposomics_m1', 'two_sixty_two_thirty')
    op.drop_column('exposomics_m1', 'two_sixty_two_eighty')
    op.drop_column('exposomics_m1', 'treatment_type')
    op.drop_column('exposomics_m1', 'tracking_number')
    op.drop_column('exposomics_m1', 'total_yield_ng')
    op.drop_column('exposomics_m1', 'total_concentration_ng_ul')
    op.drop_column('exposomics_m1', 'study_name')
    op.drop_column('exposomics_m1', 'sample_type')
    op.drop_column('exposomics_m1', 'sample_id')
    op.drop_column('exposomics_m1', 'rqs')
    op.drop_column('exposomics_m1', 'quantity_ul')
    op.drop_column('exposomics_m1', 'parent_sample_id')
    op.drop_column('exposomics_m1', 'package_id')
    op.drop_column('exposomics_m1', 'ny_flag')
    op.drop_column('exposomics_m1', 'matrix_id')
    op.drop_column('exposomics_m1', 'email')
    op.drop_column('exposomics_m1', 'contact')
    op.drop_column('exposomics_m1', 'collection_tube_id')
    op.drop_column('exposomics_m1', 'collection_date')
    op.drop_column('exposomics_m1', 'box_storageunit_id')
    op.drop_column('exposomics_m1', 'box_id_plate_id')
    op.drop_column('exposomics_m1', 'biobankid_sampleid')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
