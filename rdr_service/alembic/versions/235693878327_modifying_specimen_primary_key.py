"""modifying specimen primary key

Revision ID: 235693878327
Revises: 9bc7f48f18df
Create Date: 2020-05-05 15:43:41.847701

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
revision = '235693878327'
down_revision = '9bc7f48f18df'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    op.drop_constraint('biobank_aliquot_ibfk_1', 'biobank_aliquot', type_='foreignkey')
    op.drop_constraint('biobank_specimen_attribute_ibfk_1', 'biobank_specimen_attribute', type_='foreignkey')
    op.alter_column('biobank_specimen', 'id',
                    existing_type=mysql.INTEGER,
                    autoincrement=False,
                    nullable=False)
    op.drop_constraint('PRIMARY', 'biobank_specimen', type_='primary')
    op.alter_column('biobank_specimen', 'order_id',
               existing_type=mysql.VARCHAR(length=80),
               nullable=True)
    op.create_primary_key(None, 'biobank_specimen', ['id'])
    op.alter_column('biobank_specimen', 'id',
                    existing_type=mysql.INTEGER,
                    autoincrement=True,
                    nullable=False)
    op.create_foreign_key('biobank_aliquot_ibfk_1', 'biobank_aliquot', 'biobank_specimen', ['specimen_id'], ['id'])
    op.create_foreign_key('biobank_specimen_attribute_ibfk_1', 'biobank_specimen_attribute', 'biobank_specimen',
                          ['specimen_id'], ['id'])


def downgrade_rdr():
    op.drop_constraint('biobank_aliquot_ibfk_1', 'biobank_aliquot', type_='foreignkey')
    op.drop_constraint('biobank_specimen_attribute_ibfk_1', 'biobank_specimen_attribute', type_='foreignkey')
    op.alter_column('biobank_specimen', 'id',
                    existing_type=mysql.INTEGER,
                    autoincrement=False,
                    nullable=False)
    op.drop_constraint('PRIMARY', 'biobank_specimen', type_='primary')
    op.alter_column('biobank_specimen', 'order_id',
               existing_type=mysql.VARCHAR(length=80),
               nullable=False)
    op.create_primary_key(None, 'biobank_specimen', ['id', 'order_id'])
    op.alter_column('biobank_specimen', 'id',
                    existing_type=mysql.INTEGER,
                    autoincrement=True,
                    nullable=False)
    op.create_foreign_key('biobank_aliquot_ibfk_1', 'biobank_aliquot', 'biobank_specimen', ['specimen_id'], ['id'])
    op.create_foreign_key('biobank_specimen_attribute_ibfk_1', 'biobank_specimen_attribute', 'biobank_specimen',
                          ['specimen_id'], ['id'])


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

