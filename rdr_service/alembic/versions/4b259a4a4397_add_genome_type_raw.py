"""add_genome_type_raw

Revision ID: 4b259a4a4397
Revises: 377e8ced743f
Create Date: 2021-09-02 12:27:50.288667

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


# revision identifiers, used by Alembic.
revision = '4b259a4a4397'
down_revision = '377e8ced743f'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_aw1_raw', sa.Column('genome_type', sa.String(length=80), nullable=True))
    op.add_column('genomic_aw2_raw', sa.Column('genome_type', sa.String(length=80), nullable=True))

    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_aw2_raw', 'genome_type')
    op.drop_column('genomic_aw1_raw', 'genome_type')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
