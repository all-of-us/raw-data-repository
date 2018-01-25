"""ppi drop withdrawals

Revision ID: 574efa4de1ba
Revises: d111f9087581
Create Date: 2018-01-25 10:27:24.597642

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
revision = '574efa4de1ba'
down_revision = 'd111f9087581'
branch_labels = None
depends_on = None


_PPI_PARTICIPANT_VIEW_SQL = """
CREATE OR REPLACE VIEW ppi_participant_view AS
 SELECT
   p.participant_id,
   YEAR(p.sign_up_time) sign_up_year,
   p.suspension_status,
   YEAR(p.suspension_time) suspension_year,
   hpo.name hpo,
   /* Deidentify low population zip codes; assumes a 5-digit format. */
   IF(LENGTH(ps.zip_code) != 5, 'INVALID',
     IF(SUBSTR(ps.zip_code, 1, 3) IN (
          '036', '692', '878', '059', '790', '879', '063', '821', '884', '102',
          '823', '890', '203', '830', '893', '556', '831'),
        '000', SUBSTR(ps.zip_code, 1, 3)
     )) deidentified_zip_code,
   state_code.value state,
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
     WHERE p.withdrawal_status = 1 AND # NOT_WITHDRAWN
           (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
           (hpo.name IS NULL OR hpo.name != 'TEST')
"""

_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL = """
CREATE OR REPLACE VIEW questionnaire_response_answer_view AS
 SELECT
   p.participant_id participant_id,
   YEAR(p.sign_up_time) participant_sign_up_year,
   p.suspension_status participant_suspension_status,
   YEAR(p.suspension_time) participant_suspension_year,
   hpo.name hpo,
   (SELECT GROUP_CONCAT(concept_code.value SEPARATOR ',') FROM questionnaire_concept qc
     INNER JOIN code concept_code ON concept_code.code_id = qc.code_id
     WHERE qc.questionnaire_id = q.questionnaire_id) module,
   q.questionnaire_id questionnaire_id,
   qr.questionnaire_response_id questionnaire_response_id,
   YEAR(qr.created) questionnaire_response_submission_year,
   qc.value question_code,
   qc.code_id question_code_id,
   YEAR(qra.end_time) answer_end_year,
   ac.value answer_code,
   ac.code_id answer_code_id,
   qra.value_boolean answer_boolean,
   IF(qra.value_decimal IS NULL, 0, 1) answer_decimal_present,
   /* Integer values are potentially identifying, so whitelist questions. */
   IF(qc.value IN (
        'LivingSituation_HowManyPeople',
        'LivingSituation_PeopleUnder18',
        'Smoking_AverageDailyCigaretteNumber',
        'AttemptQuitSmoking_CompletelyQuitAge',
        'Smoking_DailySmokeStartingAge',
        'Smoking_CurrentDailyCigaretteNumber',
        'Smoking_NumberOfYears',
        'OutsideTravel6Month_OutsideTravel6MonthHowLong',
        'OverallHealth_AveragePain7Days',
        'OverallHealthOvaryRemovalHistoryAge',
        'OverallHealth_HysterectomyHistoryAge'),
      qra.value_integer, NULL) answer_integer,
   IF(qra.value_integer IS NULL, 0, 1) answer_integer_present,
   IF(qra.value_string IS NULL, 0, 1) answer_string_present,
   YEAR(qra.value_date) answer_date_year,
   YEAR(qra.value_datetime) answer_datetime_year,
   IF(qra.value_uri IS NULL, 0, 1) answer_uri_present
 FROM
   participant p
    INNER JOIN questionnaire_response qr ON p.participant_id = qr.participant_id
    INNER JOIN questionnaire_response_answer qra
       ON qra.questionnaire_response_id = qr.questionnaire_response_id
    INNER JOIN questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id
    INNER JOIN questionnaire q ON qq.questionnaire_id = q.questionnaire_id
    INNER JOIN code qc ON qq.code_id = qc.code_id
    INNER JOIN participant_summary ps ON p.participant_id = ps.participant_id
    LEFT OUTER JOIN hpo ON p.hpo_id = hpo.hpo_id
    LEFT OUTER JOIN code ac ON qra.value_code_id = ac.code_id
    WHERE p.withdrawal_status = 1 AND # NOT_WITHDRAWN
        (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
        (hpo.name IS NULL OR hpo.name != 'TEST') AND
        /* Blacklist any codes which are superfluous for PPI QA. */
        qc.value NOT IN (
          'ExtraConsent_DataSharingVideo',
          'ExtraConsent_EmailACopyToMe',
          'ExtraConsent_FitnessTrackerVideo',
          'ExtraConsent_HealthDataVideo',
          'ExtraConsent_HealthRecordVideo',
          'ExtraConsent_KeepinginTouchVideo',
          'ExtraConsent_OtherHealthDataVideo',
          'ExtraConsent_PhysicalMeausrementsVideo', # Intentional typo
          'ExtraConsent_WelcomeVideo',
          'YoutubeVideos_WhatAreWeAsking',
          'YoutubeVideos_RiskToPrivacy'
        )
"""


_MEASUREMENTS_VIEW_SQL = """
CREATE OR REPLACE VIEW physical_measurements_view AS
 SELECT
   p.participant_id participant_id,
   YEAR(p.sign_up_time) participant_sign_up_year,
   p.suspension_status participant_suspension_status,
   YEAR(p.suspension_time) participant_suspension_year,
   pm.physical_measurements_id physical_measurements_id,
   YEAR(pm.created) created_year,
   pm.amended_measurements_id amended_measurements_id,
   pm.created_site_id created_site_id,
   pm.created_username created_username,
   pm.finalized_site_id finalized_site_id,
   pm.finalized_username finalized_username,
   m.measurement_id measurement_id,
   m.code_system code_system,
   m.code_value code_value,
   YEAR(m.measurement_time) measurement_year,
   m.body_site_code_system body_site_code_system,
   m.body_site_code_value body_site_code_value,
   IF(m.value_string IS NULL, 0, 1) value_string_present,
   m.value_decimal value_decimal,
   m.value_unit value_unit,
   m.value_code_system value_code_system,
   m.value_code_value value_code_value,
   YEAR(m.value_datetime) value_datetime_year,
   m.parent_id parent_id,
   m.qualifier_id qualifier_id
 FROM
   participant p
    INNER JOIN physical_measurements pm
       ON pm.participant_id = p.participant_id
    INNER JOIN measurement m
       ON pm.physical_measurements_id = m.physical_measurements_id
    INNER JOIN participant_summary ps
       ON p.participant_id = ps.participant_id
    LEFT OUTER JOIN hpo
       ON p.hpo_id = hpo.hpo_id
  WHERE p.withdrawal_status = 1 AND # NOT_WITHDRAWN
      (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
      (hpo.name IS NULL OR hpo.name != 'TEST') AND
      pm.final = 1
"""

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    op.execute(_PPI_PARTICIPANT_VIEW_SQL)
    op.execute(_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL)
    op.execute(_MEASUREMENTS_VIEW_SQL)


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
