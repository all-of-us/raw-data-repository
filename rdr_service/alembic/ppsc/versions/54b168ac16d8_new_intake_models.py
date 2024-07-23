"""new_intake_models

Revision ID: 54b168ac16d8
Revises: 98b0562e5d0d
Create Date: 2024-07-22 11:14:50.034401

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '54b168ac16d8'
down_revision = '98b0562e5d0d'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_ppsc():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('deactivation_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_name', sa.String(length=128), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('data_element_name', sa.String(length=512), nullable=True),
    sa.Column('data_element_value', sa.String(length=512), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=512), nullable=True),
    sa.Column('is_correction_flag', mysql.TINYINT(), nullable=True),
    sa.Column('dev_note', sa.String(length=512), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['ppsc.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['ppsc.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_index(op.f('ix_ppsc_deactivation_event_created'), 'deactivation_event',
                    ['created'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_deactivation_event_data_element_name'), 'deactivation_event',
                    ['data_element_name'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_deactivation_event_data_element_value'), 'deactivation_event',
                    ['data_element_value'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_deactivation_event_event_authored_time'), 'deactivation_event',
                    ['event_authored_time'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_deactivation_event_event_type_name'), 'deactivation_event',
                    ['event_type_name'], unique=False, schema='ppsc')
    op.create_table('participant_status_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_name', sa.String(length=128), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('data_element_name', sa.String(length=512), nullable=True),
    sa.Column('data_element_value', sa.String(length=512), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=512), nullable=True),
    sa.Column('is_correction_flag', mysql.TINYINT(), nullable=True),
    sa.Column('dev_note', sa.String(length=512), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['ppsc.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['ppsc.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_index(op.f('ix_ppsc_participant_status_event_created'), 'participant_status_event',
                    ['created'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_participant_status_event_data_element_name'), 'participant_status_event',
                    ['data_element_name'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_participant_status_event_data_element_value'), 'participant_status_event',
                    ['data_element_value'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_participant_status_event_event_authored_time'), 'participant_status_event',
                    ['event_authored_time'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_participant_status_event_event_type_name'), 'participant_status_event',
                    ['event_type_name'], unique=False, schema='ppsc')
    op.create_table('site_attribution_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_name', sa.String(length=128), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('data_element_name', sa.String(length=512), nullable=True),
    sa.Column('data_element_value', sa.String(length=512), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=512), nullable=True),
    sa.Column('is_correction_flag', mysql.TINYINT(), nullable=True),
    sa.Column('dev_note', sa.String(length=512), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['ppsc.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['ppsc.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_index(op.f('ix_ppsc_site_attribution_event_created'), 'site_attribution_event',
                    ['created'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_attribution_event_data_element_name'), 'site_attribution_event',
                    ['data_element_name'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_attribution_event_data_element_value'), 'site_attribution_event',
                    ['data_element_value'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_attribution_event_event_authored_time'), 'site_attribution_event',
                    ['event_authored_time'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_site_attribution_event_event_type_name'), 'site_attribution_event',
                    ['event_type_name'], unique=False, schema='ppsc')
    op.create_table('withdrawal_event',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('modified', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('participant_id', sa.BigInteger(), nullable=True),
    sa.Column('event_id', sa.BigInteger(), nullable=True),
    sa.Column('event_type_name', sa.String(length=128), nullable=True),
    sa.Column('event_authored_time', rdr_service.model.utils.UTCDateTime(), nullable=True),
    sa.Column('data_element_name', sa.String(length=512), nullable=True),
    sa.Column('data_element_value', sa.String(length=512), nullable=True),
    sa.Column('ignore_flag', mysql.TINYINT(), nullable=True),
    sa.Column('ignore_reason', sa.String(length=512), nullable=True),
    sa.Column('is_correction_flag', mysql.TINYINT(), nullable=True),
    sa.Column('dev_note', sa.String(length=512), nullable=True),
    sa.ForeignKeyConstraint(['event_id'], ['ppsc.participant_event_activity.id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['ppsc.participant.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='ppsc'
    )
    op.create_index(op.f('ix_ppsc_withdrawal_event_created'), 'withdrawal_event',
                    ['created'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_withdrawal_event_data_element_name'), 'withdrawal_event',
                    ['data_element_name'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_withdrawal_event_data_element_value'), 'withdrawal_event',
                    ['data_element_value'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_withdrawal_event_event_authored_time'), 'withdrawal_event',
                    ['event_authored_time'], unique=False, schema='ppsc')
    op.create_index(op.f('ix_ppsc_withdrawal_event_event_type_name'), 'withdrawal_event',
                    ['event_type_name'], unique=False, schema='ppsc')
    op.drop_constraint('consent_event_ibfk_2', 'consent_event', type_='foreignkey')
    op.drop_constraint('consent_event_ibfk_1', 'consent_event', type_='foreignkey')
    op.create_foreign_key(None, 'consent_event', 'participant_event_activity', ['event_id'],
                          ['id'], source_schema='ppsc', referent_schema='ppsc')
    op.create_foreign_key(None, 'consent_event', 'participant', ['participant_id'], ['id'],
                          source_schema='ppsc', referent_schema='ppsc')
    op.drop_constraint('enrollment_event_ibfk_2', 'enrollment_event', type_='foreignkey')
    op.drop_constraint('enrollment_event_ibfk_1', 'enrollment_event', type_='foreignkey')
    op.drop_constraint('enrollment_event_ibfk_3', 'enrollment_event', type_='foreignkey')
    op.create_foreign_key(None, 'enrollment_event', 'enrollment_event_type', ['event_type_id'],
                          ['id'], source_schema='ppsc', referent_schema='ppsc')
    op.create_foreign_key(None, 'enrollment_event', 'participant_event_activity', ['event_id'],
                          ['id'], source_schema='ppsc', referent_schema='ppsc')
    op.create_foreign_key(None, 'enrollment_event', 'participant', ['participant_id'], ['id'],
                          source_schema='ppsc', referent_schema='ppsc')
    op.drop_constraint('participant_event_activity_ibfk_1', 'participant_event_activity', type_='foreignkey')
    op.drop_constraint('participant_event_activity_ibfk_2', 'participant_event_activity', type_='foreignkey')
    op.create_foreign_key(None, 'participant_event_activity', 'participant', ['participant_id'],
                          ['id'], source_schema='ppsc', referent_schema='ppsc')
    op.create_foreign_key(None, 'participant_event_activity', 'activity', ['activity_id'], ['id'],
                          source_schema='ppsc', referent_schema='ppsc')
    op.drop_constraint('profile_updates_event_ibfk_1', 'profile_updates_event', type_='foreignkey')
    op.drop_constraint('profile_updates_event_ibfk_2', 'profile_updates_event', type_='foreignkey')
    op.create_foreign_key(None, 'profile_updates_event', 'participant_event_activity', ['event_id'],
                          ['id'], source_schema='ppsc', referent_schema='ppsc')
    op.create_foreign_key(None, 'profile_updates_event', 'participant', ['participant_id'], ['id'],
                          source_schema='ppsc', referent_schema='ppsc')
    op.drop_constraint('survey_completion_event_ibfk_1', 'survey_completion_event', type_='foreignkey')
    op.drop_constraint('survey_completion_event_ibfk_2', 'survey_completion_event', type_='foreignkey')
    op.create_foreign_key(None, 'survey_completion_event', 'participant_event_activity', ['event_id'],
                          ['id'], source_schema='ppsc', referent_schema='ppsc')
    op.create_foreign_key(None, 'survey_completion_event', 'participant', ['participant_id'], ['id'],
                          source_schema='ppsc', referent_schema='ppsc')
    # ### end Alembic commands ###

    op.execute(
        """
        INSERT INTO ppsc.activity (created, modified, name, ignore_flag)
        VALUES
        (now(), now(), 'Withdrawal', 0),
        (now(), now(), 'Deactivation', 0),
        (now(), now(), 'Participant Status', 0),
        (now(), now(), 'Site Attribution', 0)
        ;
        """
    )


def downgrade_ppsc():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'survey_completion_event', schema='ppsc', type_='foreignkey')
    op.drop_constraint(None, 'survey_completion_event', schema='ppsc', type_='foreignkey')
    op.create_foreign_key('survey_completion_event_ibfk_2', 'survey_completion_event',
                          'participant', ['participant_id'], ['id'])
    op.create_foreign_key('survey_completion_event_ibfk_1', 'survey_completion_event',
                          'participant_event_activity', ['event_id'], ['id'])
    op.drop_constraint(None, 'profile_updates_event', schema='ppsc', type_='foreignkey')
    op.drop_constraint(None, 'profile_updates_event', schema='ppsc', type_='foreignkey')
    op.create_foreign_key('profile_updates_event_ibfk_2', 'profile_updates_event', 'participant',
                          ['participant_id'], ['id'])
    op.create_foreign_key('profile_updates_event_ibfk_1', 'profile_updates_event',
                          'participant_event_activity', ['event_id'], ['id'])
    op.drop_constraint(None, 'participant_event_activity', schema='ppsc', type_='foreignkey')
    op.drop_constraint(None, 'participant_event_activity', schema='ppsc', type_='foreignkey')
    op.create_foreign_key('participant_event_activity_ibfk_2', 'participant_event_activity',
                          'participant', ['participant_id'], ['id'])
    op.create_foreign_key('participant_event_activity_ibfk_1', 'participant_event_activity',
                          'activity', ['activity_id'], ['id'])
    op.drop_constraint(None, 'enrollment_event', schema='ppsc', type_='foreignkey')
    op.drop_constraint(None, 'enrollment_event', schema='ppsc', type_='foreignkey')
    op.drop_constraint(None, 'enrollment_event', schema='ppsc', type_='foreignkey')
    op.create_foreign_key('enrollment_event_ibfk_3', 'enrollment_event',
                          'participant_event_activity',
                          ['event_id'], ['id'])
    op.create_foreign_key('enrollment_event_ibfk_1', 'enrollment_event',
                          'enrollment_event_type', ['event_type_id'], ['id'])
    op.create_foreign_key('enrollment_event_ibfk_2', 'enrollment_event',
                          'participant', ['participant_id'], ['id'])
    op.drop_constraint(None, 'consent_event', schema='ppsc', type_='foreignkey')
    op.drop_constraint(None, 'consent_event', schema='ppsc', type_='foreignkey')
    op.create_foreign_key('consent_event_ibfk_1', 'consent_event', 'participant_event_activity', ['event_id'], ['id'])
    op.create_foreign_key('consent_event_ibfk_2', 'consent_event', 'participant', ['participant_id'], ['id'])
    op.drop_index(op.f('ix_ppsc_withdrawal_event_event_type_name'), table_name='withdrawal_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_withdrawal_event_event_authored_time'), table_name='withdrawal_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_withdrawal_event_data_element_value'), table_name='withdrawal_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_withdrawal_event_data_element_name'), table_name='withdrawal_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_withdrawal_event_created'), table_name='withdrawal_event', schema='ppsc')
    op.drop_table('withdrawal_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_attribution_event_event_type_name'),
                  table_name='site_attribution_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_attribution_event_event_authored_time'),
                  table_name='site_attribution_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_attribution_event_data_element_value'),
                  table_name='site_attribution_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_attribution_event_data_element_name'),
                  table_name='site_attribution_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_site_attribution_event_created'),
                  table_name='site_attribution_event', schema='ppsc')
    op.drop_table('site_attribution_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_participant_status_event_event_type_name'),
                  table_name='participant_status_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_participant_status_event_event_authored_time'),
                  table_name='participant_status_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_participant_status_event_data_element_value'),
                  table_name='participant_status_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_participant_status_event_data_element_name'),
                  table_name='participant_status_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_participant_status_event_created'),
                  table_name='participant_status_event', schema='ppsc')
    op.drop_table('participant_status_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_deactivation_event_event_type_name'),
                  table_name='deactivation_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_deactivation_event_event_authored_time'),
                  table_name='deactivation_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_deactivation_event_data_element_value'),
                  table_name='deactivation_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_deactivation_event_data_element_name'),
                  table_name='deactivation_event', schema='ppsc')
    op.drop_index(op.f('ix_ppsc_deactivation_event_created'),
                  table_name='deactivation_event', schema='ppsc')
    op.drop_table('deactivation_event', schema='ppsc')
    # ### end Alembic commands ###
