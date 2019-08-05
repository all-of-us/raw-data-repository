"""Add short value

Revision ID: 8d12872e0b77
Revises: 76d21e039dfd
Create Date: 2017-09-15 13:30:39.443982

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from sqlalchemy.dialects import mysql

from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus
from rdr_service.participant_enums import WithdrawalStatus, SuspensionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from rdr_service.model.code import CodeType

# revision identifiers, used by Alembic.
revision = '8d12872e0b77'
down_revision = '76d21e039dfd'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('code', sa.Column('short_value', sa.String(length=50), nullable=True))
    op.add_column('code_history', sa.Column('short_value', sa.String(length=50), nullable=True))
    op.alter_column('participant_summary', 'city',
               existing_type=mysql.VARCHAR(length=80),
               type_=sa.String(length=255),
               existing_nullable=True)
    op.alter_column('participant_summary', 'first_name',
               existing_type=mysql.VARCHAR(length=80),
               type_=sa.String(length=255),
               existing_nullable=False)
    op.alter_column('participant_summary', 'last_name',
               existing_type=mysql.VARCHAR(length=80),
               type_=sa.String(length=255),
               existing_nullable=False)
    op.alter_column('participant_summary', 'middle_name',
               existing_type=mysql.VARCHAR(length=80),
               type_=sa.String(length=255),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('participant_summary', 'middle_name',
               existing_type=sa.String(length=255),
               type_=mysql.VARCHAR(length=80),
               existing_nullable=True)
    op.alter_column('participant_summary', 'last_name',
               existing_type=sa.String(length=255),
               type_=mysql.VARCHAR(length=80),
               existing_nullable=False)
    op.alter_column('participant_summary', 'first_name',
               existing_type=sa.String(length=255),
               type_=mysql.VARCHAR(length=80),
               existing_nullable=False)
    op.alter_column('participant_summary', 'city',
               existing_type=sa.String(length=255),
               type_=mysql.VARCHAR(length=80),
               existing_nullable=True)
    op.drop_column('code_history', 'short_value')
    op.drop_column('code', 'short_value')
    # ### end Alembic commands ###
