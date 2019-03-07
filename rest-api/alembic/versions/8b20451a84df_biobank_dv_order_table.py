"""Biobank DV order table

Revision ID: 8b20451a84df
Revises: 80d36c1e37e2
Create Date: 2019-03-07 08:41:20.962729

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from sqlalchemy.dialects import mysql

from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from participant_enums import MetricSetType, MetricsKey
from model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus
from model.code import CodeType
from model.base import add_table_history_table

# revision identifiers, used by Alembic.
revision = '8b20451a84df'
down_revision = '80d36c1e37e2'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('biobank_dv_order',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', mysql.DATETIME(fsp=6), nullable=False),
    sa.Column('modified', mysql.DATETIME(fsp=6), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.Column('order_date', sa.Date(), nullable=True),
    sa.Column('supplier', sa.String(length=80), nullable=True),
    sa.Column('supplier_status', sa.String(length=30), nullable=True),
    sa.Column('item_name', sa.String(length=80), nullable=True),
    sa.Column('item_sku_code', sa.String(length=80), nullable=True),
    sa.Column('item_snomed_code', sa.String(length=80), nullable=True),
    sa.Column('item_quantity', sa.Integer(), nullable=True),
    sa.Column('street_address_1', sa.String(length=255), nullable=True),
    sa.Column('street_address_2', sa.String(length=255), nullable=True),
    sa.Column('city', sa.String(length=255), nullable=True),
    sa.Column('state_id', sa.Integer(), nullable=True),
    sa.Column('zip_code', sa.String(length=10), nullable=True),
    sa.Column('biobank_street_address_1', sa.String(length=255), nullable=True),
    sa.Column('biobank_street_address_2', sa.String(length=255), nullable=True),
    sa.Column('biobank_city', sa.String(length=255), nullable=True),
    sa.Column('biobank_state_id', sa.Integer(), nullable=True),
    sa.Column('biobank_zip_code', sa.String(length=10), nullable=True),
    sa.Column('shipment_last_update', sa.Date(), nullable=True),
    sa.Column('tracking_id', sa.String(length=80), nullable=True),
    sa.Column('biobank_tracking_id', sa.String(length=80), nullable=True),
    sa.Column('order_type', sa.String(length=80), nullable=True),
    sa.Column('order_status', model.utils.Enum(OrderShipmentStatus), nullable=True),
    sa.Column('shipment_carrier', sa.String(length=80), nullable=True),
    sa.Column('shipment_est_arrival', sa.Date(), nullable=True),
    sa.Column('shipment_status', model.utils.Enum(OrderShipmentTrackingStatus), nullable=True),
    sa.Column('barcode', sa.String(length=80), nullable=True),
    sa.Column('biobank_order_id', sa.String(length=80), nullable=True),
    sa.Column('biobank_reference', sa.String(length=80), nullable=True),
    sa.Column('biobank_status', sa.String(length=30), nullable=True),
    sa.Column('biobank_received', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('biobank_requisition', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['biobank_order_id'], ['biobank_order.biobank_order_id'], ),
    sa.ForeignKeyConstraint(['biobank_state_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['state_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('participant_id', 'order_id', name='uidx_partic_id_order_id')
    )
    op.execute('ALTER TABLE biobank_dv_order CHANGE COLUMN `created` `default` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);')
    op.execute('ALTER TABLE biobank_dv_order CHANGE COLUMN `modified` `modified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);')
    # ### end Alembic commands ###

    # Create a history table for 'biobank_dv_order'.
    add_table_history_table('biobank_dv_order', op)

def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('biobank_dv_order')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

