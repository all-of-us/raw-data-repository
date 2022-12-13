"""add_category_swap

Revision ID: 9d4ad0768cfd
Revises: 10ea8456cbb0
Create Date: 2022-06-08 12:46:14.934362

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from rdr_service.genomic_enums import GenomicSampleSwapCategory

# revision identifiers, used by Alembic.
revision = '9d4ad0768cfd'
down_revision = '10ea8456cbb0'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_sample_swap_member',
                  sa.Column('category', rdr_service.model.utils.Enum(GenomicSampleSwapCategory), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_sample_swap_member', 'category')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
