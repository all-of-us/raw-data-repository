"""Reduce URI field size

Revision ID: 94fa5d266916
Revises: c37d49853d1b
Create Date: 2020-06-11 13:48:23.673943

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
revision = '94fa5d266916'
down_revision = 'c37d49853d1b'
branch_labels = None
depends_on = None

DROP_INDEX_SQL = """
ALTER TABLE resource_search_results DROP FOREIGN KEY resource_search_results_ibfk_1;
ALTER TABLE resource_data DROP FOREIGN KEY resource_data_ibfk_2;
DROP INDEX ix_res_data_type_modified_hpo_id ON resource_data;
ALTER TABLE resource_search_results
    ADD CONSTRAINT resource_search_results_ibfk_1
        FOREIGN KEY (resource_data_id) REFERENCES resource_data (id);
ALTER TABLE resource_data
    ADD CONSTRAINT resource_data_ibfk_2
        FOREIGN KEY (resource_type_id) REFERENCES resource_type (id);
"""


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(DROP_INDEX_SQL)
    op.execute('ALTER TABLE resource_data MODIFY uri varchar(1024) null')
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE resource_data MODIFY uri varchar(2024) null')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
