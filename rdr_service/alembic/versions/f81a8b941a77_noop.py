"""fix pv column name

Revision ID: f81a8b941a77
Revises: 29e344f31b21
Create Date: 2018-01-17 09:40:19.259683

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, SuspensionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from rdr_service.participant_enums import MetricSetType, MetricsKey
from rdr_service.model.site_enums import SiteStatus
from rdr_service.model.code import CodeType

# revision identifiers, used by Alembic.
revision = 'f81a8b941a77'
down_revision = '29e344f31b21'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    pass


def downgrade_rdr():
    pass


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
