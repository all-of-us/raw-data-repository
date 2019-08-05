"""genomic_set_member_validation

Revision ID: 2697860e93f0
Revises: e5d456399216
Create Date: 2019-06-10 15:26:16.257259

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from rdr_service.model.genomics import GenomicValidationFlag

from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = '2697860e93f0'
down_revision = 'e5d456399216'
branch_labels = None
depends_on = None


# The `genomic_set_member.validation_status` has been split into two columns now

# first populate the new column from the old values
COPY_VALIDATION_FLAGS = '''
update genomic_set_member set validation_flags=if(validation_status > 1, validation_status, null);
'''

# then clean the old values in the existing column
CLEAN_VALIDATION_STATUS = '''
update genomic_set_member set validation_status=if(validation_status > 1, 2, validation_status);
'''


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_set_member', sa.Column('validation_flags', model.utils.MultiEnum(GenomicValidationFlag), nullable=True))
    # ### end Alembic commands ###
    op.execute(COPY_VALIDATION_FLAGS)
    op.execute(CLEAN_VALIDATION_STATUS)


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_set_member', 'validation_flags')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

