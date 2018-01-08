"""Participant views

Revision ID: 106ecd33248b
Revises: fddd3f850e2c
Create Date: 2018-01-08 11:24:41.814336

"""
from alembic import op
import sqlalchemy as sa
import model.utils


from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType
from participant_enums import MetricSetType, MetricsKey
from model.site_enums import SiteStatus
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = '106ecd33248b'
down_revision = 'fddd3f850e2c'
branch_labels = None
depends_on = None

_PPI_PARTICIPANT_VIEW_SQL = """
CREATE OR REPLACE VIEW ppi_participant_view AS
 SELECT
   p.participant_id,
   YEAR(p.sign_up_time) sign_up_year,
   p.withdrawal_status,
   YEAR(p.withdrawal_time) withdrawal_year,
   p.suspension_status,
   YEAR(p.suspension_time) suspension_year,
   hpo.name hpo,
   IF(SUBSTR(ps.zip_code, 1, 3) IN (
        '036', '692', '878', '059', '790', '879', '063', '821', '884', '102',
        '823', '890', '203', '830', '893', '556', '831'),
      '000', SUBSTR(ps.zip_code, 1, 3)
   ) deidentified_zip_code,
   state_code.value state,
   recontact_method_code.value recontact_method,
   language_code.value language,
   LEAST(89, TIMESTAMPDIFF(YEAR, ps.date_of_birth, CURDATE())) age_years,
   gender_code.value gender,
   sex_code.value sex,
   sexual_orientation_code.value sexual_orientation,
   education_code.value education,
   income_code.value income,
   ps.enrollment_status,
   ps.race,
   ps.physical_measurements_status,
   YEAR(ps.physical_measurements_finalized_time) physical_measurements_finalized_year,
   YEAR(ps.physical_measurements_time) physical_measurements_year,
   ps.physical_measurements_created_site_id,
   ps.physical_measurements_finalized_site_id,
   ps.consent_for_study_enrollment,
   YEAR(ps.consent_for_study_enrollment_time) consent_for_study_enrollment_year,
   ps.consent_for_electronic_health_records,
   YEAR(ps.consent_for_electronic_health_records_time) consent_for_electronic_health_records_year,
   ps.questionnaire_on_overall_health,
   YEAR(ps.questionnaire_on_overall_health_time) questionnaire_on_overall_health_year,
   ps.questionnaire_on_lifestyle,
   YEAR(ps.questionnaire_on_lifestyle_time) questionnaire_on_lifestyle_year,
   ps.questionnaire_on_the_basics,
   YEAR(ps.questionnaire_on_the_basics_time) questionnaire_on_the_basics_year,
   ps.questionnaire_on_healthcare_access,
   YEAR(ps.questionnaire_on_healthcare_access_time) questionnaire_on_healthcare_access_year,
   ps.questionnaire_on_medical_history,
   YEAR(ps.questionnaire_on_medical_history_time) questionnaire_on_medical_history_year,
   ps.questionnaire_on_medications,
   YEAR(ps.questionnaire_on_medications_time) questionnaire_on_medications_year,
   ps.questionnaire_on_family_health,
   YEAR(ps.questionnaire_on_family_health_time) questionnaire_on_family_health_year,
   ps.biospecimen_status,
   YEAR(ps.biospecimen_order_time) biospecimen_order_year,
   ps.biospecimen_source_site_id,
   ps.biospecimen_collected_site_id,
   ps.biospecimen_processed_site_id,
   ps.biospecimen_finalized_site_id,
   ps.sample_order_status_1sst8,
   YEAR(ps.sample_order_status_1sst8_time) sample_order_status_1sst8_year,
   ps.sample_order_status_1pst8,
   YEAR(ps.sample_order_status_1pst8_time) sample_order_status_1pst8_year,
   ps.sample_order_status_1hep4,
   YEAR(ps.sample_order_status_1hep4_time) sample_order_status_1hep4_year,
   ps.sample_order_status_1ed04,
   YEAR(ps.sample_order_status_1ed04_time) sample_order_status_1ed04_year,
   ps.sample_order_status_1ed10,
   YEAR(ps.sample_order_status_1ed10_time) sample_order_status_1ed10_year,
   ps.sample_order_status_2ed10,
   YEAR(ps.sample_order_status_2ed10_time) sample_order_status_2ed10_year,
   ps.sample_order_status_1ur10,
   YEAR(ps.sample_order_status_1ur10_time) sample_order_status_1ur10_year,
   ps.sample_order_status_1sal,
   YEAR(ps.sample_order_status_1sal_time) sample_order_status_1sal_year,
   ps.sample_status_1sst8,
   YEAR(ps.sample_status_1sst8_time) sample_status_1sst8_year,
   ps.sample_status_1pst8,
   YEAR(ps.sample_status_1pst8_time) sample_status_1pst8_year,
   ps.sample_status_1hep4,
   YEAR(ps.sample_status_1hep4_time) sample_status_1hep4_year,
   ps.sample_status_1ed04,
   YEAR(ps.sample_status_1ed04_time) sample_status_1ed04_year,
   ps.sample_status_1ed10,
   YEAR(ps.sample_status_1ed10_time) sample_status_1ed10_year,
   ps.sample_status_2ed10,
   YEAR(ps.sample_status_2ed10_time) sample_status_2ed10_year,
   ps.sample_status_1ur10,
   YEAR(ps.sample_status_1ur10_time) sample_status_1ur10_year,
   ps.sample_status_1sal,
   YEAR(ps.sample_status_1sal_time) sample_status_1sal_year,
   ps.num_completed_baseline_ppi_modules,
   ps.num_completed_ppi_modules,
   ps.num_baseline_samples_arrived,
   ps.samples_to_isolate_dna,
   ps.consent_for_cabor,
   YEAR(ps.consent_for_cabor_time) consent_for_cabor_year,
   (SELECT IFNULL(GROUP_CONCAT(
      IF(ac.value = 'WhatRaceEthnicity_RaceEthnicityNoneOfThese',
         'NoneOfThese',
         TRIM(LEADING 'WhatRaceEthnicity_' FROM
              TRIM(LEADING 'PMI_' FROM ac.value)))),
    'None')
    FROM questionnaire_response qr, questionnaire_response_answer qra,
      questionnaire_question qq, code c, code ac
    WHERE qra.end_time IS NULL AND
          qr.questionnaire_response_id = qra.questionnaire_response_id AND
          qra.question_id = qq.questionnaire_question_id AND
          qq.code_id = c.code_id AND c.value = 'Race_WhatRaceEthnicity' AND
          qr.participant_id = p.participant_id AND
          qra.value_code_id = ac.code_id AND
          ac.value != 'WhatRaceEthnicity_Hispanic'
   ) race_codes,
   (SELECT COUNT(ac.value)
    FROM questionnaire_response qr, questionnaire_response_answer qra,
      questionnaire_question qq, code c, code ac
    WHERE qra.end_time IS NULL AND
      qr.questionnaire_response_id = qra.questionnaire_response_id AND
      qra.question_id = qq.questionnaire_question_id AND
      qq.code_id = c.code_id AND c.value = 'Race_WhatRaceEthnicity' AND
      qr.participant_id = p.participant_id AND
      qra.value_code_id = ac.code_id AND
      ac.value = 'WhatRaceEthnicity_Hispanic'
   ) hispanic
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

_PARTICIPANT_VIEW_SQL = """
CREATE OR REPLACE VIEW participant_view AS
 SELECT
   p.participant_id,
   p.sign_up_time,
   p.withdrawal_status,
   p.withdrawal_time,
   p.suspension_status,
   p.suspension_time,
   hpo.name hpo,
   ps.zip_code,
   state_code.value state,
   recontact_method_code.value recontact_method,
   language_code.value language,
   TIMESTAMPDIFF(YEAR, ps.date_of_birth, CURDATE()) age_years,
   gender_code.value gender,
   sex_code.value sex,
   sexual_orientation_code.value sexual_orientation,
   education_code.value education,
   income_code.value income,
   ps.enrollment_status,
   ps.race,
   ps.physical_measurements_status,
   ps.physical_measurements_finalized_time,
   ps.physical_measurements_time,
   ps.physical_measurements_created_site_id,
   ps.physical_measurements_finalized_site_id,
   ps.consent_for_study_enrollment,
   ps.consent_for_study_enrollment_time,
   ps.consent_for_electronic_health_records,
   ps.consent_for_electronic_health_records_time,
   ps.questionnaire_on_overall_health,
   ps.questionnaire_on_overall_health_time questionnaire_on_overall_health_year,
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
   ps.biospecimen_status,
   ps.biospecimen_order_time,
   ps.biospecimen_source_site_id,
   ps.biospecimen_collected_site_id,
   ps.biospecimen_processed_site_id,
   ps.biospecimen_finalized_site_id,
   ps.sample_order_status_1sst8,
   ps.sample_order_status_1sst8_time,
   ps.sample_order_status_1pst8,
   ps.sample_order_status_1pst8_time,
   ps.sample_order_status_1hep4,
   ps.sample_order_status_1hep4_time,
   ps.sample_order_status_1ed04,
   ps.sample_order_status_1ed04_time,
   ps.sample_order_status_1ed10,
   ps.sample_order_status_1ed10_time,
   ps.sample_order_status_2ed10,
   ps.sample_order_status_2ed10_time,
   ps.sample_order_status_1ur10,
   ps.sample_order_status_1ur10_time,
   ps.sample_order_status_1sal,
   ps.sample_order_status_1sal_time,
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
   ps.consent_for_cabor_time,
   (SELECT IFNULL(GROUP_CONCAT(
      IF(ac.value = 'WhatRaceEthnicity_RaceEthnicityNoneOfThese',
         'NoneOfThese',
         TRIM(LEADING 'WhatRaceEthnicity_' FROM
              TRIM(LEADING 'PMI_' FROM ac.value)))),
    'None')
    FROM questionnaire_response qr, questionnaire_response_answer qra,
      questionnaire_question qq, code c, code ac
    WHERE qra.end_time IS NULL AND
          qr.questionnaire_response_id = qra.questionnaire_response_id AND
          qra.question_id = qq.questionnaire_question_id AND
          qq.code_id = c.code_id AND c.value = 'Race_WhatRaceEthnicity' AND
          qr.participant_id = p.participant_id AND
          qra.value_code_id = ac.code_id AND
          ac.value != 'WhatRaceEthnicity_Hispanic'
   ) race_codes,
   (SELECT COUNT(ac.value)
    FROM questionnaire_response qr, questionnaire_response_answer qra,
      questionnaire_question qq, code c, code ac
    WHERE qra.end_time IS NULL AND
      qr.questionnaire_response_id = qra.questionnaire_response_id AND
      qra.question_id = qq.questionnaire_question_id AND
      qq.code_id = c.code_id AND c.value = 'Race_WhatRaceEthnicity' AND
      qr.participant_id = p.participant_id AND
      qra.value_code_id = ac.code_id AND
      ac.value = 'WhatRaceEthnicity_Hispanic'
   ) hispanic
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

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

def upgrade_rdr():
   op.execute(_PPI_PARTICIPANT_VIEW_SQL)
   op.execute(_PARTICIPANT_VIEW_SQL)

def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

