"""emoji_question_answer

Revision ID: 41824183e8e3
Revises: 71ae16b33f24
Create Date: 2021-03-15 16:55:49.341236

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
revision = '41824183e8e3'
down_revision = '71ae16b33f24'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        "ALTER TABLE `questionnaire_response_answer` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    op.execute(
        "ALTER TABLE `participant_summary` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        "ALTER TABLE `questionnaire_response_answer` CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci;")
    op.execute(
        "ALTER TABLE `participant_summary` CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci;")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
