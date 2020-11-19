"""create genomic_manifest_file and genomic_manifest_feedback

Revision ID: 4318d20e8422
Revises: ba05cd337cdc
Create Date: 2020-11-19 11:34:35.688360

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils

# revision identifiers, used by Alembic.
from rdr_service.participant_enums import GenomicManifestTypes

revision = '4318d20e8422'
down_revision = 'ba05cd337cdc'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    # New table: genomic_manifest_file
    op.create_table('genomic_manifest_file',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
        sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=False),
        sa.Column('upload_date', rdr_service.model.utils.UTCDateTime(), nullable=True),
        sa.Column('manifest_type_id', rdr_service.model.utils.Enum(GenomicManifestTypes), nullable=True),
        sa.Column('file_path', sa.String(length=255), nullable=True),
        sa.Column('bucket_name', sa.String(length=128), nullable=True),
        sa.Column('record_count', sa.Integer(), nullable=False),
        sa.Column('rdr_processing_complete', sa.SmallInteger(), nullable=False),
        sa.Column('rdr_processing_complete_date', rdr_service.model.utils.UTCDateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # New table: genomic_manifest_feedback
    op.create_table('genomic_manifest_feedback',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=False),
        sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=False),
        sa.Column('input_manifest_file_id', sa.Integer(), nullable=False),
        sa.Column('feedback_manifest_file_id', sa.Integer(), nullable=True),
        sa.Column('feedback_record_count', sa.Integer(), nullable=False),
        sa.Column('feedback_complete', sa.SmallInteger(), nullable=False),
        sa.Column('feedback_complete_date', rdr_service.model.utils.UTCDateTime(), nullable=True),
        sa.ForeignKeyConstraint(['feedback_manifest_file_id'], ['genomic_manifest_file.id'], ),
        sa.ForeignKeyConstraint(['input_manifest_file_id'], ['genomic_manifest_file.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    op.add_column('genomic_file_processed', sa.Column('genomic_manifest_file_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'genomic_file_processed', 'genomic_manifest_file', ['genomic_manifest_file_id'], ['id'])
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'genomic_file_processed', type_='foreignkey')
    op.drop_column('genomic_file_processed', 'genomic_manifest_file_id')
    op.drop_table('genomic_manifest_feedback')
    op.drop_table('genomic_manifest_file')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

