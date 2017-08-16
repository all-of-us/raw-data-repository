"""Filter test participants

Revision ID: ffcd82a35890
Revises: 9b70eae79af0
Create Date: 2017-08-15 16:17:22.245919

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = 'ffcd82a35890'
down_revision = '9b70eae79af0'
branch_labels = None
depends_on = None

_PARTICIPANT_VIEW_SQL = """
CREATE OR REPLACE VIEW participant_view AS
 SELECT
   p.sign_up_time,
   p.withdrawal_status,
   p.withdrawal_time,
   p.suspension_status,
   p.suspension_time,
   hpo.name hpo,
   ps.zip_code,
   state_code.value state,
   ps.city,
   recontact_method_code.value recontact_method,
   language_code.value language,
   ps.date_of_birth,
   gender_code.value gender,
   sex_code.value sex,
   sexual_orientation_code.value sexual_orientation,
   education_code.value education,
   income_code.value income,
   ps.enrollment_status,
   ps.race,
   ps.physical_measurements_status,
   ps.physical_measurements_time,
   ps.consent_for_study_enrollment,
   ps.consent_for_study_enrollment_time,
   ps.consent_for_electronic_health_records,
   ps.consent_for_electronic_health_records_time,
   ps.questionnaire_on_overall_health,
   ps.questionnaire_on_overall_health_time,
   ps.questionnaire_on_lifestyle,
   ps.questionnaire_on_lifestyle_time,
   ps.questionnaire_on_the_basics,
   ps.questionnaire_on_the_basics_time,
   ps.questionnaire_on_healthcare_access,
   ps.questionnaire_on_healthcare_access_time,
   ps.questionnaire_on_medical_history,
   ps.questionnaire_on_medical_history_time,
   ps.questionnaire_on_medications,
   ps.questionnaire_on_medications_time,
   ps.questionnaire_on_family_health,
   ps.questionnaire_on_family_health_time,
   ps.sample_status_1sst8,
   ps.sample_status_1sst8_time,
   ps.sample_status_1pst8,
   ps.sample_status_1pst8_time,
   ps.sample_status_1hep4,
   ps.sample_status_1hep4_time,
   ps.sample_status_1ed04,
   ps.sample_status_1ed04_time,
   ps.sample_status_1ed10,
   ps.sample_status_1ed10_time,
   ps.sample_status_2ed10,
   ps.sample_status_2ed10_time,
   ps.sample_status_1ur10,
   ps.sample_status_1ur10_time,
   ps.sample_status_1sal,
   ps.sample_status_1sal_time,
   ps.num_completed_baseline_ppi_modules,
   ps.num_completed_ppi_modules,
   ps.num_baseline_samples_arrived,
   ps.samples_to_isolate_dna,
   ps.consent_for_cabor,
   ps.consent_for_cabor_time
 FROM
   participant p
     LEFT OUTER JOIN hpo ON p.hpo_id = hpo.hpo_id
     LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
     LEFT OUTER JOIN code state_code ON ps.state_id = state_code.code_id
     LEFT OUTER JOIN code recontact_method_code ON ps.recontact_method_id = recontact_method_code.code_id
     LEFT OUTER JOIN code language_code ON ps.language_id = language_code.code_id
     LEFT OUTER JOIN code gender_code ON ps.gender_identity_id = gender_code.code_id
     LEFT OUTER JOIN code sex_code ON ps.sex_id = sex_code.code_id
     LEFT OUTER JOIN code sexual_orientation_code ON ps.sexual_orientation_id = sexual_orientation_code.code_id
     LEFT OUTER JOIN code education_code ON ps.education_id = education_code.code_id
     LEFT OUTER JOIN code income_code ON ps.income_id = income_code.code_id
     WHERE (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
           (hpo.name IS NULL OR hpo.name != 'TEST')        
"""
def upgrade():
  op.execute(_PARTICIPANT_VIEW_SQL)

def downgrade():
    pass

