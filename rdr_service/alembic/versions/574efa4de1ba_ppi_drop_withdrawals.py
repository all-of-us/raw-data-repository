"""ppi drop withdrawals

Revision ID: 574efa4de1ba
Revises: d111f9087581
Create Date: 2018-01-25 10:27:24.597642

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "574efa4de1ba"
down_revision = "d111f9087581"
branch_labels = None
depends_on = None

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
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()


def upgrade_rdr():
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
