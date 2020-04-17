"""resource tables

Revision ID: e7c8cbc247ff
Revises: 49751eda40fd
Create Date: 2020-04-17 09:04:53.389669

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

import model.utils
from rdr_service.model.resource_type import ResourceTypeEnum

# revision identifiers, used by Alembic.
revision = 'e7c8cbc247ff'
down_revision = '49751eda40fd'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('resource_type',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('resource_uri', sa.String(length=80), nullable=False),
    sa.Column('resource_Key', sa.String(length=80), nullable=False),
    sa.Column('resource_key_id', model.utils.Enum(ResourceTypeEnum), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('resource_uri')
    )

    op.create_table('resource_schema',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('resource_type_id', sa.BigInteger(), nullable=False),
    sa.Column('schema', mysql.JSON(), nullable=False),
    sa.ForeignKeyConstraint(['resource_type_id'], ['resource_type.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('resource_type_id', 'modified')
    )

    op.create_table('resource_data',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('resource_type_id', sa.BigInteger(), nullable=False),
    sa.Column('resource_schema_id', sa.BigInteger(), nullable=False),
    sa.Column('hpo_id', sa.Integer(), nullable=True),
    sa.Column('resource_pk', sa.String(length=65), nullable=True),
    sa.Column('resource_pk_id', sa.Integer(), nullable=True),
    sa.Column('resource_pk_alt_id', sa.String(length=80), nullable=True),
    sa.Column('parent_id', sa.BigInteger(), nullable=True),
    sa.Column('parent_type_id', sa.BigInteger(), nullable=True),
    sa.Column('resource', mysql.JSON(), nullable=False),
    sa.ForeignKeyConstraint(['resource_schema_id'], ['resource_schema.id'], ),
    sa.ForeignKeyConstraint(['resource_type_id'], ['resource_type.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_res_data_type_modified_hpo_id', 'resource_data', ['resource_type_id', 'modified', 'hpo_id'], unique=False)

    op.create_table('resource_search_results',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('search_key', sa.Integer(), nullable=False),
    sa.Column('pageNo', sa.Integer(), nullable=False),
    sa.Column('resource_data_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['resource_data_id'], ['resource_data.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_res_data_type_modified_hpo_id', 'resource_search_results', ['search_key', 'pageNo'], unique=False)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_res_data_type_modified_hpo_id', table_name='resource_search_results')
    op.drop_table('resource_search_results')
    op.drop_index('ix_res_data_type_modified_hpo_id', table_name='resource_data')
    op.drop_table('resource_data')
    op.drop_table('resource_schema')
    op.drop_table('resource_type')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

