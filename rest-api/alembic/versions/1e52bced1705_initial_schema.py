"""Initial schema

Revision ID: 1e52bced1705
Revises: 
Create Date: 2017-03-03 09:48:55.385009

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from participant_enums import HPOId, PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import MembershipTier
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = '1e52bced1705'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('code_book',
    sa.Column('code_book_id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('latest', sa.Boolean(), nullable=False),
    sa.Column('name', sa.String(length=80), nullable=False),
    sa.Column('system', sa.String(length=255), nullable=False),
    sa.Column('version', sa.String(length=80), nullable=False),
    sa.PrimaryKeyConstraint('code_book_id'),
    sa.UniqueConstraint('system', 'version')
    )
    op.create_table('hpo',
    sa.Column('hpo_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('name', sa.String(length=20), nullable=True),
    sa.PrimaryKeyConstraint('hpo_id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('log_position',
    sa.Column('log_position_id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('log_position_id')
    )
    op.create_table('metrics_version',
    sa.Column('metrics_version_id', sa.Integer(), nullable=False),
    sa.Column('in_progress', sa.Boolean(), nullable=False),
    sa.Column('complete', sa.Boolean(), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=False),
    sa.Column('data_version', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('metrics_version_id')
    )
    op.create_table('questionnaire',
    sa.Column('questionnaire_id', sa.Integer(), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('last_modified', sa.DateTime(), nullable=False),
    sa.Column('resource', sa.BLOB(), nullable=False),
    sa.PrimaryKeyConstraint('questionnaire_id')
    )
    op.create_table('questionnaire_history',
    sa.Column('questionnaire_id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('last_modified', sa.DateTime(), nullable=False),
    sa.Column('resource', sa.BLOB(), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('questionnaire_id', 'version')
    )
    op.create_table('code',
    sa.Column('code_id', sa.Integer(), nullable=False),
    sa.Column('system', sa.String(length=255), nullable=False),
    sa.Column('value', sa.String(length=80), nullable=False),
    sa.Column('display', sa.UnicodeText(), nullable=True),
    sa.Column('topic', sa.UnicodeText(), nullable=True),
    sa.Column('code_type', model.utils.Enum(CodeType), nullable=False),
    sa.Column('mapped', sa.Boolean(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('code_book_id', sa.Integer(), nullable=True),
    sa.Column('parent_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['code_book_id'], ['code_book.code_book_id'], ),
    sa.ForeignKeyConstraint(['parent_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('code_id'),
    sa.UniqueConstraint('value')
    )
    op.create_table('metrics_bucket',
    sa.Column('metrics_version_id', sa.Integer(), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('hpo_id', sa.String(length=20), nullable=False),
    sa.Column('metrics', sa.BLOB(), nullable=False),
    sa.ForeignKeyConstraint(['metrics_version_id'], ['metrics_version.metrics_version_id'], ),
    sa.PrimaryKeyConstraint('metrics_version_id', 'date', 'hpo_id')
    )
    op.create_table('participant',
    sa.Column('participant_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('biobank_id', sa.Integer(), nullable=False),
    sa.Column('last_modified', sa.DateTime(), nullable=False),
    sa.Column('sign_up_time', sa.DateTime(), nullable=False),
    sa.Column('provider_link', sa.BLOB(), nullable=True),
    sa.Column('client_id', sa.String(length=80), nullable=True),
    sa.Column('hpo_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['hpo_id'], ['hpo.hpo_id'], ),
    sa.PrimaryKeyConstraint('participant_id')
    )
    op.create_index('participant_biobank_id', 'participant', ['biobank_id'], unique=True)
    op.create_index('participant_hpo_id', 'participant', ['hpo_id'], unique=False)
    op.create_table('participant_history',
    sa.Column('participant_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('biobank_id', sa.Integer(), nullable=False),
    sa.Column('last_modified', sa.DateTime(), nullable=False),
    sa.Column('sign_up_time', sa.DateTime(), nullable=False),
    sa.Column('provider_link', sa.BLOB(), nullable=True),
    sa.Column('client_id', sa.String(length=80), nullable=True),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('hpo_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['hpo_id'], ['hpo.hpo_id'], ),
    sa.PrimaryKeyConstraint('participant_id', 'version')
    )
    op.create_table('biobank_order',
    sa.Column('biobank_order_id', sa.Integer(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('source_site_system', sa.String(length=80), nullable=True),
    sa.Column('source_site_value', sa.String(length=80), nullable=True),
    sa.Column('collected', sa.UnicodeText(), nullable=True),
    sa.Column('processed', sa.UnicodeText(), nullable=True),
    sa.Column('finalized', sa.UnicodeText(), nullable=True),
    sa.Column('log_position_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['log_position_id'], ['log_position.log_position_id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('biobank_order_id')
    )
    op.create_table('biobank_stored_sample',
    sa.Column('biobank_stored_sample_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=True),
    sa.Column('family_id', sa.String(length=80), nullable=True),
    sa.Column('sample_id', sa.String(length=80), nullable=True),
    sa.Column('storage_status', sa.String(length=80), nullable=True),
    sa.Column('type', sa.String(length=80), nullable=True),
    sa.Column('test_code', sa.String(length=80), nullable=True),
    sa.Column('treatments', sa.String(length=80), nullable=True),
    sa.Column('expected_volume', sa.String(length=80), nullable=True),
    sa.Column('quantity', sa.String(length=80), nullable=True),
    sa.Column('container_type', sa.String(length=80), nullable=True),
    sa.Column('collection_date', sa.DateTime(), nullable=True),
    sa.Column('disposal_status', sa.String(length=80), nullable=True),
    sa.Column('disposed_date', sa.DateTime(), nullable=True),
    sa.Column('parent_sample_id', sa.Integer(), nullable=True),
    sa.Column('confirmed_date', sa.DateTime(), nullable=True),
    sa.Column('log_position_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['log_position_id'], ['log_position.log_position_id'], ),
    sa.ForeignKeyConstraint(['parent_sample_id'], ['biobank_stored_sample.biobank_stored_sample_id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('biobank_stored_sample_id')
    )
    op.create_table('code_history',
    sa.Column('system', sa.String(length=255), nullable=False),
    sa.Column('value', sa.String(length=80), nullable=False),
    sa.Column('display', sa.UnicodeText(), nullable=True),
    sa.Column('topic', sa.UnicodeText(), nullable=True),
    sa.Column('code_type', model.utils.Enum(CodeType), nullable=False),
    sa.Column('mapped', sa.Boolean(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('code_history_id', sa.Integer(), nullable=False),
    sa.Column('code_id', sa.Integer(), nullable=True),
    sa.Column('code_book_id', sa.Integer(), nullable=True),
    sa.Column('parent_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['code_book_id'], ['code_book.code_book_id'], ),
    sa.ForeignKeyConstraint(['parent_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('code_history_id'),
    sa.UniqueConstraint('code_book_id', 'code_id'),
    sa.UniqueConstraint('code_book_id', 'value')
    )
    op.create_table('participant_summary',
    sa.Column('participant_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('biobank_id', sa.Integer(), nullable=False),
    sa.Column('first_name', sa.String(length=80), nullable=True),
    sa.Column('middle_name', sa.String(length=80), nullable=True),
    sa.Column('last_name', sa.String(length=80), nullable=True),
    sa.Column('zip_code', sa.String(length=10), nullable=True),
    sa.Column('date_of_birth', sa.Date(), nullable=True),
    sa.Column('gender_identity_id', sa.Integer(), nullable=True),
    sa.Column('membership_tier', model.utils.Enum(MembershipTier), nullable=True),
    sa.Column('race_id', sa.Integer(), nullable=True),
    sa.Column('ethnicity_id', sa.Integer(), nullable=True),
    sa.Column('physical_measurements_status', model.utils.Enum(PhysicalMeasurementsStatus), nullable=True),
    sa.Column('sign_up_time', sa.DateTime(), nullable=True),
    sa.Column('hpo_id', sa.Integer(), nullable=False),
    sa.Column('consent_for_study_enrollment', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('consent_for_study_enrollment_time', sa.DateTime(), nullable=True),
    sa.Column('consent_for_electronic_health_records', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('consent_for_electronic_health_records_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_overall_health', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_overall_health_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_personal_habits', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_personal_habits_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_sociodemographics', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_sociodemographics_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_healthcare_access', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_healthcare_access_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_medical_history', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_medical_history_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_medications', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_medications_time', sa.DateTime(), nullable=True),
    sa.Column('questionnaire_on_family_health', model.utils.Enum(QuestionnaireStatus), nullable=True),
    sa.Column('questionnaire_on_family_health_time', sa.DateTime(), nullable=True),
    sa.Column('num_completed_baseline_ppi_modules', sa.SmallInteger(), nullable=True),
    sa.Column('num_baseline_samples_arrived', sa.SmallInteger(), nullable=True),
    sa.ForeignKeyConstraint(['ethnicity_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['gender_identity_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['hpo_id'], ['hpo.hpo_id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['race_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('participant_id')
    )
    op.create_index('participant_summary_biobank_id', 'participant_summary', ['biobank_id'], unique=False)
    op.create_index('participant_summary_hpo', 'participant_summary', ['hpo_id'], unique=False)
    op.create_index('participant_summary_hpo_consent', 'participant_summary', ['hpo_id', 'consent_for_study_enrollment'], unique=False)
    op.create_index('participant_summary_hpo_dob', 'participant_summary', ['hpo_id', 'date_of_birth'], unique=False)
    op.create_index('participant_summary_hpo_ethnicity', 'participant_summary', ['hpo_id', 'ethnicity_id'], unique=False)
    op.create_index('participant_summary_hpo_fn', 'participant_summary', ['hpo_id', 'first_name'], unique=False)
    op.create_index('participant_summary_hpo_ln', 'participant_summary', ['hpo_id', 'last_name'], unique=False)
    op.create_index('participant_summary_hpo_num_baseline_ppi', 'participant_summary', ['hpo_id', 'num_completed_baseline_ppi_modules'], unique=False)
    op.create_index('participant_summary_hpo_num_baseline_samples', 'participant_summary', ['hpo_id', 'num_baseline_samples_arrived'], unique=False)
    op.create_index('participant_summary_hpo_tier', 'participant_summary', ['hpo_id', 'membership_tier'], unique=False)
    op.create_index('participant_summary_hpo_zip', 'participant_summary', ['hpo_id', 'zip_code'], unique=False)
    op.create_index('participant_summary_ln_dob', 'participant_summary', ['last_name', 'date_of_birth'], unique=False)
    op.create_index('participant_summary_ln_dob_fn', 'participant_summary', ['last_name', 'date_of_birth', 'first_name'], unique=False)
    op.create_index('participant_summary_ln_dob_zip', 'participant_summary', ['last_name', 'date_of_birth', 'zip_code'], unique=False)
    op.create_table('physical_measurements',
    sa.Column('physical_measurements_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('resource', sa.BLOB(), nullable=False),
    sa.Column('final', sa.Boolean(), nullable=False),
    sa.Column('amended_measurements_id', sa.Integer(), nullable=True),
    sa.Column('log_position_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['amended_measurements_id'], ['physical_measurements.physical_measurements_id'], ),
    sa.ForeignKeyConstraint(['log_position_id'], ['log_position.log_position_id'], ),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.PrimaryKeyConstraint('physical_measurements_id')
    )
    op.create_table('questionnaire_concept',
    sa.Column('questionnaire_concept_id', sa.Integer(), nullable=False),
    sa.Column('questionnaire_id', sa.Integer(), nullable=False),
    sa.Column('questionnaire_version', sa.Integer(), nullable=False),
    sa.Column('code_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['code_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], ['questionnaire_history.questionnaire_id', 'questionnaire_history.version'], ),
    sa.PrimaryKeyConstraint('questionnaire_concept_id'),
    sa.UniqueConstraint('questionnaire_id', 'questionnaire_version', 'code_id')
    )
    op.create_table('questionnaire_question',
    sa.Column('questionnaire_question_id', sa.Integer(), nullable=False),
    sa.Column('questionnaire_id', sa.Integer(), nullable=True),
    sa.Column('questionnaire_version', sa.Integer(), nullable=True),
    sa.Column('link_id', sa.String(length=20), nullable=True),
    sa.Column('code_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['code_id'], ['code.code_id'], ),
    sa.ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], ['questionnaire_history.questionnaire_id', 'questionnaire_history.version'], ),
    sa.PrimaryKeyConstraint('questionnaire_question_id'),
    sa.UniqueConstraint('questionnaire_id', 'questionnaire_version', 'link_id')
    )
    op.create_table('questionnaire_response',
    sa.Column('questionnaire_response_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('questionnaire_id', sa.Integer(), nullable=False),
    sa.Column('questionnaire_version', sa.Integer(), nullable=False),
    sa.Column('participant_id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('resource', sa.BLOB(), nullable=False),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.participant_id'], ),
    sa.ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], ['questionnaire_history.questionnaire_id', 'questionnaire_history.version'], ),
    sa.PrimaryKeyConstraint('questionnaire_response_id')
    )
    op.create_table('biobank_order_identifier',
    sa.Column('system', sa.String(length=80), nullable=False),
    sa.Column('value', sa.String(length=80), nullable=False),
    sa.Column('order_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['order_id'], ['biobank_order.biobank_order_id'], ),
    sa.PrimaryKeyConstraint('system', 'value')
    )
    op.create_table('biobank_ordered_sample',
    sa.Column('order_id', sa.Integer(), nullable=False),
    sa.Column('test', sa.String(length=80), nullable=False),
    sa.Column('description', sa.UnicodeText(), nullable=False),
    sa.Column('processing_required', sa.Boolean(), nullable=False),
    sa.Column('collected', sa.DateTime(), nullable=True),
    sa.Column('processed', sa.DateTime(), nullable=True),
    sa.Column('finalized', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['order_id'], ['biobank_order.biobank_order_id'], ),
    sa.PrimaryKeyConstraint('order_id', 'test')
    )
    op.create_table('questionnaire_response_answer',
    sa.Column('questionnaire_response_answer_id', sa.Integer(), nullable=False),
    sa.Column('questionnaire_response_id', sa.Integer(), nullable=False),
    sa.Column('question_id', sa.Integer(), nullable=False),
    sa.Column('end_time', sa.DateTime(), nullable=True),
    sa.Column('value_system', sa.String(length=50), nullable=True),
    sa.Column('value_code_id', sa.Integer(), nullable=True),
    sa.Column('value_boolean', sa.Boolean(), nullable=True),
    sa.Column('value_decimal', sa.Float(), nullable=True),
    sa.Column('value_integer', sa.Integer(), nullable=True),
    sa.Column('value_string', sa.String(length=1024), nullable=True),
    sa.Column('value_date', sa.Date(), nullable=True),
    sa.Column('value_datetime', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['question_id'], ['questionnaire_question.questionnaire_question_id'], ),
    sa.ForeignKeyConstraint(['questionnaire_response_id'], ['questionnaire_response.questionnaire_response_id'], ),
    sa.ForeignKeyConstraint(['value_code_id'], ['code.code_id'], ),
    sa.PrimaryKeyConstraint('questionnaire_response_answer_id')
    )
    # ### end Alembic commands ###
    
    hpo_table = sa.Table('hpo', sa.MetaData(),
      sa.Column('hpo_id', sa.Integer(), autoincrement=False, nullable=False),
      sa.Column('name', sa.String(length=20), nullable=True),
      sa.PrimaryKeyConstraint('hpo_id'),
      sa.UniqueConstraint('name')
    )

    # Insert our HPO IDs into the HPO table.
    op.bulk_insert(hpo_table,
    [
        {'hpo_id': 0, 'name': 'UNSET' },
        {'hpo_id': 1, 'name': 'PITT' },
        {'hpo_id': 2, 'name': 'COLUMBIA' },
        {'hpo_id': 3, 'name': 'ILLNOIS' },
        {'hpo_id': 4, 'name': 'AZ_TUCSON' },
        {'hpo_id': 5, 'name': 'COMM_HEALTH' },
        {'hpo_id': 6, 'name': 'SAN_YSIDRO' },
        {'hpo_id': 7, 'name': 'CHEROKEE' },
        {'hpo_id': 8, 'name': 'EAU_CLAIRE' },
        {'hpo_id': 9, 'name': 'HRHCARE' },
        {'hpo_id': 10, 'name': 'JACKSON' },
        {'hpo_id': 11, 'name': 'GEISINGER' },
        {'hpo_id': 12, 'name': 'CAL_PMC' },
        {'hpo_id': 13, 'name': 'NE_PMC' },
        {'hpo_id': 14, 'name': 'TRANS_AM' },
        {'hpo_id': 15, 'name': 'VA' }
    ])


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('questionnaire_response_answer')
    op.drop_table('biobank_ordered_sample')
    op.drop_table('biobank_order_identifier')
    op.drop_table('questionnaire_response')
    op.drop_table('questionnaire_question')
    op.drop_table('questionnaire_concept')
    op.drop_table('physical_measurements')
    op.drop_index('participant_summary_ln_dob_zip', table_name='participant_summary')
    op.drop_index('participant_summary_ln_dob_fn', table_name='participant_summary')
    op.drop_index('participant_summary_ln_dob', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_zip', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_tier', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_num_baseline_samples', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_num_baseline_ppi', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_ln', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_fn', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_ethnicity', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_dob', table_name='participant_summary')
    op.drop_index('participant_summary_hpo_consent', table_name='participant_summary')
    op.drop_index('participant_summary_hpo', table_name='participant_summary')
    op.drop_index('participant_summary_biobank_id', table_name='participant_summary')
    op.drop_table('participant_summary')
    op.drop_table('code_history')
    op.drop_table('biobank_stored_sample')
    op.drop_table('biobank_order')
    op.drop_table('participant_history')
    op.drop_index('participant_hpo_id', table_name='participant')
    op.drop_index('participant_biobank_id', table_name='participant')
    op.drop_table('participant')
    op.drop_table('metrics_bucket')
    op.drop_table('code')
    op.drop_table('questionnaire_history')
    op.drop_table('questionnaire')
    op.drop_table('metrics_version')
    op.drop_table('log_position')
    op.drop_table('hpo')
    op.drop_table('code_book')
    # ### end Alembic commands ###
