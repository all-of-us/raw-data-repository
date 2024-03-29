"""adding_diet_data

Revision ID: 6999197e31d9
Revises: d5ef6dd601fe
Create Date: 2023-06-29 13:21:10.673315

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

from rdr_service.ancillary_study_resources.nph.enums import ModuleTypes, DietType, DietStatus

# revision identifiers, used by Alembic.
revision = '6999197e31d9'
down_revision = 'd5ef6dd601fe'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('diet_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('diet_id', mysql.TINYINT(), nullable=False),
    sa.Column('status_id', sa.BigInteger(), nullable=False),
    sa.Column('module', rdr_service.model.utils.Enum(ModuleTypes), nullable=False),
    sa.Column('diet_name', rdr_service.model.utils.Enum(DietType), nullable=False),
    sa.Column('status', rdr_service.model.utils.Enum(DietStatus), nullable=False),
    sa.Column('current', mysql.TINYINT(), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['nph.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['nph.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='nph'
    )
    op.add_column('deactivation_event', sa.Column('module', rdr_service.model.utils.Enum(ModuleTypes), nullable=False))
    op.add_column('withdrawal_event', sa.Column('module', rdr_service.model.utils.Enum(ModuleTypes), nullable=False))
    # ### end Alembic commands ###

    op.execute(
        """
        Update nph.deactivation_event
        Set nph.deactivation_event.module = 1
        Where true
        """
    )

    op.execute(
        """
        Update nph.withdrawal_event
        Set nph.withdrawal_event.module = 1
        Where true
        """
    )


def downgrade_nph():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('withdrawal_event', 'module')
    op.drop_column('deactivation_event', 'module')
    op.drop_table('diet_event', schema='nph')
    # ### end Alembic commands ###

