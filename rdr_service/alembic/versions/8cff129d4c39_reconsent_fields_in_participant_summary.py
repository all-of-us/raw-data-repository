"""reconsent fields in participant summary

Revision ID: 8cff129d4c39
Revises: 0b6965a7606e, 204dfbf689ed
Create Date: 2022-07-20 10:35:26.555216

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
revision = '8cff129d4c39'
down_revision = ('0b6965a7606e', '204dfbf689ed')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    op.execute("""
        ALTER TABLE participant_summary
        ADD COLUMN reconsent_for_study_enrollment SMALLINT,
        ADD COLUMN reconsent_for_study_enrollment_authored DATETIME,
        ADD COLUMN reconsent_for_electronic_health_records SMALLINT,
        ADD COLUMN reconsent_for_electronic_health_records_authored DATETIME
    """)


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'reconsent_for_study_enrollment_authored')
    op.drop_column('participant_summary', 'reconsent_for_study_enrollment')
    op.drop_column('participant_summary', 'reconsent_for_electronic_health_records_authored')
    op.drop_column('participant_summary', 'reconsent_for_electronic_health_records')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

