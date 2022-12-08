"""adding index for questionnaire response

Revision ID: 625fa91e6bac
Revises: 88ad431e793f, 241803b2c2d2
Create Date: 2021-05-21 09:14:49.739249

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import Connection
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
revision = '625fa91e6bac'
down_revision = ('88ad431e793f', '241803b2c2d2')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # Index seems to have been manually created in prod, creating for environments that don't have it
    connection: Connection = op.get_bind()
    index_exists = connection.scalar("show index from questionnaire_response where key_name = 'idx_created_q_id'")
    if not index_exists:
        op.create_index('idx_created_q_id', 'questionnaire_response', ['questionnaire_id', 'created'], unique=False)


def downgrade_rdr():
    op.drop_index('idx_created_q_id', table_name='questionnaire_response')


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
