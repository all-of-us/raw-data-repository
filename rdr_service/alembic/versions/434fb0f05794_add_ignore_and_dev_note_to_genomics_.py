"""add ignore and dev note to genomics models.

Revision ID: 434fb0f05794
Revises: 994dfe6e53ee
Create Date: 2020-09-30 14:39:16.244636

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '434fb0f05794'
down_revision = '994dfe6e53ee'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_gc_validation_metrics', sa.Column('dev_note', sa.String(length=255), nullable=True))
    op.add_column('genomic_gc_validation_metrics', sa.Column('ignore_flag', sa.SmallInteger(), nullable=True))

    op.add_column('genomic_set_member', sa.Column('dev_note', sa.String(length=255), nullable=True))
    op.add_column('genomic_set_member_history', sa.Column('dev_note', sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('genomic_set_member', 'dev_note')
    op.drop_column('genomic_set_member_history', 'dev_note')

    op.drop_column('genomic_gc_validation_metrics', 'ignore_flag')
    op.drop_column('genomic_gc_validation_metrics', 'dev_note')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
