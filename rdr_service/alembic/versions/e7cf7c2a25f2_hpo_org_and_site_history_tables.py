"""hpo, org, and site history tables

Revision ID: e7cf7c2a25f2
Revises: 912e25bad291, 93779a760cc3
Create Date: 2022-01-11 10:44:06.868686

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
revision = 'e7cf7c2a25f2'
down_revision = ('912e25bad291', '93779a760cc3')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    add_table_history_table('hpo', op=op, id_field_name='hpo_id')
    op.drop_index('name', table_name='hpo_history')

    add_table_history_table('organization', op=op, id_field_name='organization_id')

    add_table_history_table('site', op=op, id_field_name='site_id')
    op.drop_index('google_group', table_name='site_history')


def downgrade_rdr():
    drop_table_history_table('hpo', op=op)
    drop_table_history_table('organization', op=op)
    drop_table_history_table('site', op=op)


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
