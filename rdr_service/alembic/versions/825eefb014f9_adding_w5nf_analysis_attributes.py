"""adding_w5nf_analysis_attributes

Revision ID: 825eefb014f9
Revises: 6cb9405f1549, 57515daf8448, 33b34f5ae271
Create Date: 2022-03-30 15:51:16.904983

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
revision = '825eefb014f9'
down_revision = ('6cb9405f1549', '57515daf8448', '33b34f5ae271')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_w5nf_raw',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('file_path', sa.String(length=255), nullable=True),
    sa.Column('ignore_flag', sa.SmallInteger(), nullable=False),
    sa.Column('dev_note', sa.String(length=255), nullable=True),
    sa.Column('biobank_id', sa.String(length=255), nullable=True),
    sa.Column('sample_id', sa.String(length=255), nullable=True),
    sa.Column('request_reason', sa.String(length=255), nullable=True),
    sa.Column('request_reason_free', sa.String(length=512), nullable=True),
    sa.Column('health_related_data_file_name', sa.String(length=255), nullable=True),
    sa.Column('clinical_analysis_type', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_genomic_w5nf_raw_file_path'), 'genomic_w5nf_raw', ['file_path'], unique=False)
    op.add_column('genomic_cvl_analysis', sa.Column('failed', sa.Integer(), nullable=False))
    op.add_column('genomic_cvl_analysis', sa.Column('failed_request_reason', sa.String(length=255), nullable=True))
    op.add_column('genomic_cvl_analysis', sa.Column('failed_request_reason_free', sa.String(length=512), nullable=True))
    op.add_column('genomic_set_member', sa.Column('cvl_w5nf_hdr_manifest_job_run_id', sa.Integer(), nullable=True))
    op.add_column('genomic_set_member', sa.Column('cvl_w5nf_pgx_manifest_job_run_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['cvl_w5nf_pgx_manifest_job_run_id'], ['id'])
    op.create_foreign_key(None, 'genomic_set_member', 'genomic_job_run', ['cvl_w5nf_hdr_manifest_job_run_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.drop_constraint(None, 'genomic_set_member', type_='foreignkey')
    op.drop_column('genomic_set_member', 'cvl_w5nf_pgx_manifest_job_run_id')
    op.drop_column('genomic_set_member', 'cvl_w5nf_hdr_manifest_job_run_id')
    op.drop_column('genomic_cvl_analysis', 'failed_request_reason_free')
    op.drop_column('genomic_cvl_analysis', 'failed_request_reason')
    op.drop_column('genomic_cvl_analysis', 'failed')
    op.drop_index(op.f('ix_genomic_w5nf_raw_file_path'), table_name='genomic_w5nf_raw')
    op.drop_table('genomic_w5nf_raw')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
