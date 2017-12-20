"""Deidentify ppi views

Revision ID: 04564bf1b5e8
Revises: 6f9266e7a5fb
Create Date: 2017-12-20 13:21:37.487290

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
revision = '04564bf1b5e8'
down_revision = '6f9266e7a5fb'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL = """
CREATE OR REPLACE VIEW questionnaire_response_answer_view AS
 SELECT
   p.participant_id participant_id,
   p.sign_up_time participant_sign_up_time,
   p.withdrawal_status participant_withdrawal_status,
   p.withdrawal_time participant_withdrawal_time,
   p.suspension_status participant_suspension_status,
   p.suspension_time participant_suspension_time,
   hpo.name hpo,
   (SELECT GROUP_CONCAT(concept_code.value SEPARATOR ',') FROM questionnaire_concept qc
     INNER JOIN code concept_code ON concept_code.code_id = qc.code_id
     WHERE qc.questionnaire_id = q.questionnaire_id) module,
   q.questionnaire_id questionnaire_id,
   q.created questionnaire_creation_time,
   qr.questionnaire_response_id questionnaire_response_id,
   qr.created questionnaire_response_submission_time,
   qc.value question_code,
   qc.code_id question_code_id,
   qra.end_time answer_end_time,
   ac.value answer_code,
   ac.code_id answer_code_id,
   qra.value_boolean answer_boolean,
   qra.value_decimal answer_decimal,
   qra.value_integer answer_integer,
   qra.value_string answer_string,
   qra.value_date answer_date,
   qra.value_datetime answer_datetime,
   qra.value_uri answer_uri
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
    WHERE (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
           (hpo.name IS NULL OR hpo.name != 'TEST')
"""

_MEASUREMENTS_VIEW_SQL = """
CREATE VIEW physical_measurements_view AS
 SELECT
   p.participant_id participant_id,
   p.sign_up_time participant_sign_up_time,
   p.withdrawal_status participant_withdrawal_status,
   p.withdrawal_time participant_withdrawal_time,
   p.suspension_status participant_suspension_status,
   p.suspension_time participant_suspension_time,
   pm.physical_measurements_id physical_measurements_id,
   pm.created created,
   pm.amended_measurements_id amended_measurements_id,
   pm.created_site_id created_site_id,
   pm.created_username created_username,
   pm.finalized_site_id finalized_site_id,
   pm.finalized_username finalized_username,
   m.measurement_id measurement_id,
   m.code_system code_system,
   m.code_value code_value,
   m.measurement_time measurement_time,
   m.body_site_code_system body_site_code_system,
   m.body_site_code_value body_site_code_value,
   m.value_string value_string,
   m.value_decimal value_decimal,
   m.value_unit value_unit,
   m.value_code_system value_code_system,
   m.value_code_value value_code_value,
   m.value_datetime value_datetime,
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
  WHERE (ps.email IS NULL OR ps.email NOT LIKE '%@example.com') AND
        (hpo.name IS NULL OR hpo.name != 'TEST') AND
        pm.final = 1
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

def upgrade_rdr():
  op.execute(_MEASUREMENTS_VIEW_SQL)
  op.execute(_PARTICIPANT_VIEW_SQL)
  op.execute(_QUESTIONNAIRE_RESPONSE_ANSWER_VIEW_SQL)


def downgrade_rdr():
    pass


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
