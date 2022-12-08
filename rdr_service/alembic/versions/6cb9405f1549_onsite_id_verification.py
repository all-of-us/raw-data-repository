"""onsite_id_verification

Revision ID: 6cb9405f1549
Revises: f69e4a978a1f, 78ab5fe99ad1
Create Date: 2022-03-28 15:03:47.194126

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils

from rdr_service.participant_enums import OnSiteVerificationVisitType, OnSiteVerificationType

# revision identifiers, used by Alembic.
revision = '6cb9405f1549'
down_revision = ('f69e4a978a1f', '78ab5fe99ad1')
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('onsite_id_verification',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=True),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('user_email', sa.String(length=200), nullable=True),
    sa.Column('verified_time', rdr_service.model.utils.UTCDateTime6(fsp=6), nullable=False),
    sa.Column('verification_type', rdr_service.model.utils.Enum(OnSiteVerificationType), nullable=False),
    sa.Column('visit_type', rdr_service.model.utils.Enum(OnSiteVerificationVisitType), nullable=False),
    sa.Column('resource', sa.JSON(), nullable=True),
    sa.Column('site_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['site_id'], ['site.site_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('onsite_id_verification')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
