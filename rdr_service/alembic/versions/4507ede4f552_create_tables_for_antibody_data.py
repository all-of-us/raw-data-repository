"""create tables for antibody data

Revision ID: 4507ede4f552
Revises: 8302d4762b9a
Create Date: 2020-07-27 13:35:36.510308

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
revision = '4507ede4f552'
down_revision = '8302d4762b9a'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('quest_covid_antibody_test',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('specimen_id', sa.String(length=80), nullable=True),
    sa.Column('test_code', sa.Integer(), nullable=True),
    sa.Column('test_name', sa.String(length=200), nullable=True),
    sa.Column('run_date_time', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('accession', sa.String(length=80), nullable=False),
    sa.Column('instrument_name', sa.String(length=200), nullable=True),
    sa.Column('position', sa.String(length=80), nullable=True),
    sa.Column('ingest_file_name', sa.String(length=80), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('accession')
    )
    op.create_table('quest_covid_antibody_test_result',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('accession', sa.String(length=80), nullable=False),
    sa.Column('result_name', sa.String(length=200), nullable=True),
    sa.Column('result_value', sa.String(length=200), nullable=True),
    sa.Column('ingest_file_name', sa.String(length=80), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('accession', 'result_name')
    )
    op.create_table('biobank_covid_antibody_sample',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('aou_biobank_id', sa.Integer(), nullable=True),
    sa.Column('no_aou_biobank_id', sa.String(length=80), nullable=True),
    sa.Column('sample_id', sa.String(length=80), nullable=False),
    sa.Column('matrix_tube_id', sa.Integer(), nullable=True),
    sa.Column('sample_type', sa.String(length=80), nullable=True),
    sa.Column('quantity_ul', sa.Integer(), nullable=True),
    sa.Column('storage_location', sa.String(length=200), nullable=True),
    sa.Column('collection_date', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('ingest_file_name', sa.String(length=80), nullable=True),
    sa.ForeignKeyConstraint(['aou_biobank_id'], ['participant.biobank_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('sample_id')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('biobank_covid_antibody_sample')
    op.drop_table('quest_covid_antibody_test_result')
    op.drop_table('quest_covid_antibody_test')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

