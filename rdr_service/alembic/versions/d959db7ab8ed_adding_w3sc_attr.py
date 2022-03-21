"""adding_w3sc_attr

Revision ID: d959db7ab8ed
Revises: d960a8ec8d21, f69e4a978a1f, 78ab5fe99ad1
Create Date: 2022-03-17 13:21:12.824976

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
revision = 'd959db7ab8ed'
down_revision = ('d960a8ec8d21', 'f69e4a978a1f', '78ab5fe99ad1')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_w3sc_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('dev_note', sa.String(length=255), nullable=True),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('cvl_secondary_conf_failure', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_w3sc_raw_file_path'), 'genomic_w3sc_raw', ['file_path'], unique=False)
    op.add_column('genomic_set_member', sa.Column('cvl_secondary_conf_failure', sa.String(length=255), nullable=True))
    op.add_column('genomic_set_member', sa.Column('cvl_w3sc_manifest_job_run_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['cvl_w3sc_manifest_job_run_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.drop_column('genomic_set_member', 'cvl_w3sc_manifest_job_run_id')
    op.drop_column('genomic_set_member', 'cvl_secondary_conf_failure')
    op.drop_index(op.f('ix_genomic_w3sc_raw_file_path'), table_name='genomic_w3sc_raw')
    op.drop_table('genomic_w3sc_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

