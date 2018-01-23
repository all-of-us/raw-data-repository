"""empty message

Revision ID: 403ebb6aa7af
Revises: f81a8b941a77, 0185cd07668a
Create Date: 2018-01-23 09:47:38.073814

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from participant_enums import MetricSetType, MetricsKey
from model.site_enums import SiteStatus
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = '403ebb6aa7af'
down_revision = ('f81a8b941a77', '0185cd07668a')
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
    pass


def downgrade_metrics():
    pass

