"""starting DV order table rename

Revision ID: 8cc481fa7390
Revises: 90a21cce431b
Create Date: 2020-10-29 08:27:40.849301

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
revision = '8cc481fa7390'
down_revision = '90a21cce431b'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    op.create_table(
    'biobank_mail_kit_order',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', mysql.DATETIME(fsp=6), nullable=False),
    sa.Column('modified', mysql.DATETIME(fsp=6), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.Column('order_date', mysql.DATETIME(fsp=6), nullable=True),
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
    sa.Column('shipment_last_update', mysql.DATETIME(fsp=6), nullable=True),
    sa.Column('tracking_id', sa.String(length=80), nullable=True),
    sa.Column('biobank_tracking_id', sa.String(length=80), nullable=True),
    sa.Column('order_type', sa.String(length=80), nullable=True),
    sa.Column('order_status', rdr_service.model.utils.Enum(OrderShipmentStatus), nullable=True),
    sa.Column('shipment_carrier', sa.String(length=80), nullable=True),
    sa.Column('shipment_est_arrival', mysql.DATETIME(fsp=6), nullable=True),
    sa.Column('shipment_status', rdr_service.model.utils.Enum(OrderShipmentTrackingStatus), nullable=True),
    sa.Column('barcode', sa.String(length=80), nullable=True),
    sa.Column('biobank_order_id', sa.String(length=80), nullable=True),
    sa.Column('biobank_status', sa.String(length=30), nullable=True),
    sa.Column('biobank_received', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('biobank_requisition', sa.Text(), nullable=True),
    sa.Column('is_test_sample', sa.Boolean(), nullable=True),
    sa.Column('associated_hpo_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['associated_hpo_id'], ['hpo.hpo_id'], ),
    sa.ForeignKeyConstraint(['biobank_order_id'], ['biobank_order.biobank_order_id'], ),
    sa.ForeignKeyConstraint(['biobank_state_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['state_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('biobank_order_id'),
    sa.UniqueConstraint('participant_id', 'order_id', name='uidx_partic_id_order_id')
    )

    column_names = """
        id, created, modified, version, participant_id, order_id, order_date, supplier, supplier_status,
        item_name, item_sku_code, item_snomed_code, item_quantity, street_address_1, street_address_2, city,
        state_id, zip_code, biobank_street_address_1, biobank_street_address_2, biobank_city, biobank_state_id,
        biobank_zip_code, shipment_last_update, tracking_id, biobank_tracking_id, order_type, order_status,
        shipment_carrier, shipment_est_arrival, shipment_status, barcode, biobank_order_id, biobank_status,
        biobank_received, biobank_requisition, is_test_sample
    """
    op.execute(f"""
        INSERT INTO biobank_mail_kit_order ({column_names})
        SELECT {column_names} FROM biobank_dv_order
    """)

    add_table_history_table('biobank_mail_kit_order', op)
    # History table is created from copy of existing table, so dropping constraints
    op.drop_constraint('uidx_partic_id_order_id', 'biobank_mail_kit_order_history', type_="unique")
    op.drop_constraint('biobank_order_id', 'biobank_mail_kit_order_history', type_="unique")

    history_table_columns = f'{column_names}, revision_action, revision_id, revision_dt'
    op.execute(f"""
        INSERT INTO biobank_mail_kit_order_history ({history_table_columns})
        SELECT {history_table_columns} FROM biobank_dv_order_history
    """)


def downgrade_rdr():
    op.drop_table('biobank_mail_kit_order')
    op.drop_table('biobank_mail_kit_order_history')


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
