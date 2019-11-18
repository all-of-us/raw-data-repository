"""Create Genomic tables

Revision ID: 96999791b48b
Revises: bce6d443874f
Create Date: 2019-11-08 10:55:36.443187

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from rdr_service.model import utils
from rdr_service.model.genomics import GenomicSubProcessStatus, GenomicSubProcessResult

# revision identifiers, used by Alembic.
revision = '96999791b48b'
down_revision = 'bce6d443874f'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('genomic_job',
                    sa.Column('id', sa.Integer(),
                              autoincrement=True, nullable=False),
                    sa.Column('created', sa.DateTime(), nullable=True),
                    sa.Column('modified', sa.DateTime(), nullable=True),
                    sa.Column('name', sa.String(length=80), nullable=False),
                    sa.Column('active_flag', sa.Integer(), nullable=False),
                    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('genomic_job_run',
                    sa.Column('id', sa.Integer(),
                              autoincrement=True, nullable=False),
                    sa.Column('job_id', sa.Integer(), nullable=False),
                    sa.Column('start_time', sa.DateTime(), nullable=False),
                    sa.Column('end_time', sa.DateTime(), nullable=True),
                    sa.Column('run_status',
                              utils.Enum(GenomicSubProcessStatus),
                              nullable=True),
                    sa.Column('run_result',
                              utils.Enum(GenomicSubProcessResult),
                              nullable=True),
                    sa.Column('result_message', sa.String(length=150),
                              nullable=True),
                    sa.ForeignKeyConstraint(['job_id'], ['genomic_job.id'], ),
                    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('genomic_file_processed',
                    sa.Column('id', sa.Integer(),
                              autoincrement=True, nullable=False),
                    sa.Column('run_id', sa.Integer(),
                              nullable=False),
                    sa.Column('start_time', sa.DateTime(), nullable=False),
                    sa.Column('end_time', sa.DateTime(), nullable=True),
                    sa.Column('file_path', sa.String(length=255),
                              nullable=False),
                    sa.Column('bucket_name', sa.String(length=128),
                              nullable=False),
                    sa.Column('file_name', sa.String(length=128),
                              nullable=False),
                    sa.Column('file_status',
                              utils.Enum(GenomicSubProcessStatus),
                              nullable=True),
                    sa.Column('file_result',
                              utils.Enum(GenomicSubProcessResult),
                              nullable=True),
                    sa.ForeignKeyConstraint(['run_id'],
                                            ['genomic_job_run.id'], ),
                    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('genomic_gc_validation_metrics',
                    sa.Column('id', sa.Integer(), autoincrement=True,
                              nullable=False),
                    sa.Column('genomic_set_member_id', sa.Integer(),
                              nullable=False),
                    sa.Column('genomic_file_processed_id', sa.Integer(),
                              nullable=True),
                    sa.Column('created', sa.DateTime(), nullable=True),
                    sa.Column('modified', sa.DateTime(), nullable=True),
                    sa.Column('participant_id', sa.Integer(), nullable=False),
                    sa.Column('sample_id', sa.String(length=80), nullable=True),
                    sa.Column('lims_id', sa.String(length=80), nullable=True),
                    sa.Column('call_rate', sa.Integer(), nullable=True),
                    sa.Column('mean_coverage', sa.Integer(), nullable=True),
                    sa.Column('genome_coverage', sa.Integer(), nullable=True),
                    sa.Column('contamination', sa.Integer(), nullable=True),
                    sa.Column('sex_concordance', sa.String(length=10),
                              nullable=True),
                    sa.Column('aligned_q20_bases', sa.Integer(), nullable=True),
                    sa.Column('processing_status', sa.String(length=15),
                              nullable=True),
                    sa.Column('notes', sa.String(length=128), nullable=True),
                    sa.Column('consent_for_ror', sa.String(length=10),
                              nullable=True),
                    sa.Column('withdrawn_status', sa.Integer(), nullable=True),
                    sa.Column('site_id', sa.Integer(), nullable=True),
                    sa.ForeignKeyConstraint(['genomic_file_processed_id'],
                                            ['genomic_file_processed.id'], ),
                    sa.ForeignKeyConstraint(['genomic_set_member_id'],
                                            ['genomic_set_member.id'], ),
                    sa.ForeignKeyConstraint(['participant_id'],
                                            ['participant.participant_id'], ),
                    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###

    # Timestamp columns
    op.alter_column('genomic_job', 'created',
                    existing_type=mysql.DATETIME(fsp=6),
                    nullable=True,
                    existing_server_default=sa.text(u'current_timestamp(6)'))
    op.alter_column('genomic_job', 'modified',
                    existing_type=mysql.DATETIME(fsp=6),
                    nullable=True,
                    existing_server_default=sa.text(
                      u'current_timestamp(6) ON UPDATE current_timestamp(6)'))

    op.alter_column('genomic_gc_validation_metrics', 'created',
                    existing_type=mysql.DATETIME(fsp=6),
                    nullable=True,
                    existing_server_default=sa.text(u'current_timestamp(6)'))
    op.alter_column('genomic_gc_validation_metrics', 'modified',
                    existing_type=mysql.DATETIME(fsp=6), nullable=True,
                    existing_server_default=sa.text(
                      u'current_timestamp(6) ON UPDATE current_timestamp(6)'))

    # create jobs
    op.execute(
        """
        INSERT INTO genomic_job (name, active_flag) 
        VALUES ("gc_validation_metrics_ingestion", 1);
        """
    )


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('genomic_gc_validation_metrics')
    op.drop_table('genomic_file_processed')
    op.drop_table('genomic_job_run')
    op.drop_table('genomic_job')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

