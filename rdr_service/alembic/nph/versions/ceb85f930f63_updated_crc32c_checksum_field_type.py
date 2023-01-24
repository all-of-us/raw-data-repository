"""Updated crc32c_checksum field type

Revision ID: ceb85f930f63
Revises: 21a92ab43b53
Create Date: 2023-01-11 13:58:02.613030

"""

# pylint: disable=unused-import
# pylint: disable=line-too-long
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
revision = 'ceb85f930f63'
down_revision = '89c5294e1039'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        'biobank_file_export', 'crc32c_checksum',
        type_=sa.BigInteger(), existing_type=sa.Integer(),
        nullable=False
    )
    op.drop_constraint('order_ibfk_2', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_3', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_5', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_4', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_6', 'order', type_='foreignkey')
    op.drop_constraint('order_ibfk_1', 'order', type_='foreignkey')
    op.create_foreign_key(None, 'order', 'participant', ['participant_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['amended_site'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['created_site'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['collected_site'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'order', 'study_category', ['category_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'order', 'site', ['finalized_site'], ['id'], source_schema='nph', referent_schema='nph')
    op.drop_constraint('ordered_sample_ibfk_2', 'ordered_sample', type_='foreignkey')
    op.drop_constraint('ordered_sample_ibfk_1', 'ordered_sample', type_='foreignkey')
    op.create_foreign_key(None, 'ordered_sample', 'ordered_sample', ['parent_sample_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'ordered_sample', 'order', ['order_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.drop_constraint('sample_export_ibfk_2', 'sample_export', type_='foreignkey')
    op.drop_constraint('sample_export_ibfk_1', 'sample_export', type_='foreignkey')
    op.create_foreign_key(None, 'sample_export', 'sample_update', ['sample_update_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.create_foreign_key(None, 'sample_export', 'biobank_file_export', ['export_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.drop_constraint('sample_update_ibfk_1', 'sample_update', type_='foreignkey')
    op.create_foreign_key(None, 'sample_update', 'ordered_sample', ['rdr_ordered_sample_id'], ['id'], source_schema='nph', referent_schema='nph')
    op.drop_constraint('study_category_ibfk_1', 'study_category', type_='foreignkey')
    op.create_foreign_key(None, 'study_category', 'study_category', ['parent_id'], ['id'], source_schema='nph', referent_schema='nph')
    # ### end Alembic commands ###


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        'biobank_file_export', 'crc32c_checksum',
        type_=sa.Integer(), existing_type=sa.BigInteger(),
        nullable=True
    )
    op.drop_constraint(None, 'study_category', schema='nph', type_='foreignkey')
    op.create_foreign_key('study_category_ibfk_1', 'study_category', 'study_category', ['parent_id'], ['id'])
    op.drop_constraint(None, 'sample_update', schema='nph', type_='foreignkey')
    op.create_foreign_key('sample_update_ibfk_1', 'sample_update', 'ordered_sample', ['rdr_ordered_sample_id'], ['id'])
    op.drop_constraint(None, 'sample_export', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'sample_export', schema='nph', type_='foreignkey')
    op.create_foreign_key('sample_export_ibfk_1', 'sample_export', 'biobank_file_export', ['export_id'], ['id'])
    op.create_foreign_key('sample_export_ibfk_2', 'sample_export', 'sample_update', ['sample_update_id'], ['id'])
    op.drop_constraint(None, 'ordered_sample', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'ordered_sample', schema='nph', type_='foreignkey')
    op.create_foreign_key('ordered_sample_ibfk_1', 'ordered_sample', 'ordered_sample', ['parent_sample_id'], ['id'])
    op.create_foreign_key('ordered_sample_ibfk_2', 'ordered_sample', 'order', ['order_id'], ['id'])
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.drop_constraint(None, 'order', schema='nph', type_='foreignkey')
    op.create_foreign_key('order_ibfk_1', 'order', 'site', ['created_site'], ['id'])
    op.create_foreign_key('order_ibfk_6', 'order', 'study_category', ['category_id'], ['id'])
    op.create_foreign_key('order_ibfk_4', 'order', 'site', ['collected_site'], ['id'])
    op.create_foreign_key('order_ibfk_5', 'order', 'site', ['amended_site'], ['id'])
    op.create_foreign_key('order_ibfk_3', 'order', 'site', ['finalized_site'], ['id'])
    op.create_foreign_key('order_ibfk_2', 'order', 'participant', ['participant_id'], ['id'])
    # ### end Alembic commands ###
