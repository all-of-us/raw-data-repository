"""add-ehr-metrics

Revision ID: 4825a0ad42e1
Revises: 3513057132ca
Create Date: 2019-03-04 10:31:53.101508

"""
import model.utils
import sqlalchemy as sa
from alembic import op

from rdr_service.participant_enums import EhrStatus

# revision identifiers, used by Alembic.
revision = "4825a0ad42e1"
down_revision = "3513057132ca"
branch_labels = None
depends_on = None


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
   ps.sample_order_status_1sal2,
   ps.sample_order_status_1sal2_time,
   ps.sample_order_status_1cfd9,
   ps.sample_order_status_1cfd9_time,
   ps.sample_order_status_1pxr2,
   ps.sample_order_status_1pxr2_time,
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
   ps.sample_status_1sal2,
   ps.sample_status_1sal2_time,
   ps.sample_status_1cfd9,
   ps.sample_status_1cfd9_time,
   ps.sample_status_1pxr2,
   ps.sample_status_1pxr2_time,
   ps.num_completed_baseline_ppi_modules,
   ps.num_completed_ppi_modules,
   ps.num_baseline_samples_arrived,
   ps.samples_to_isolate_dna,
   ps.consent_for_cabor,
   ps.consent_for_cabor_time,
   ps.ehr_status,
   ps.ehr_receipt_time,
   ps.ehr_update_time,
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


_RAW_PARTICIPANT_VIEW_EXPORT_SQL = """
CREATE OR REPLACE VIEW raw_ppi_participant_view AS
 SELECT
   p.participant_id,
   ps.last_name,
   ps.first_name,
   ps.email,
   p.sign_up_time,
   p.suspension_status,
   p.suspension_time,
   hpo.name hpo,
   ps.zip_code,
   state_code.value state,
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
   ps.sample_order_status_1sal2,
   ps.sample_order_status_1sal2_time,
   ps.sample_order_status_1cfd9,
   ps.sample_order_status_1cfd9_time,
   ps.sample_order_status_1pxr2,
   ps.sample_order_status_1pxr2_time,
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
   ps.sample_status_1sal2,
   ps.sample_status_1sal2_time,
   ps.sample_status_1cfd9,
   ps.sample_status_1cfd9_time,
   ps.sample_status_1pxr2,
   ps.sample_status_1pxr2_time,
   ps.num_completed_baseline_ppi_modules,
   ps.num_completed_ppi_modules,
   ps.num_baseline_samples_arrived,
   ps.samples_to_isolate_dna,
   ps.consent_for_cabor,
   ps.consent_for_cabor_time,
   ps.ehr_status,
   ps.ehr_receipt_time,
   ps.ehr_update_time,
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
   ps.sample_order_status_1sal2,
   YEAR(ps.sample_order_status_1sal2_time) sample_order_status_1sal2_year,
   ps.sample_order_status_1cfd9,
   YEAR(ps.sample_order_status_1cfd9_time) sample_order_status_1cfd9_year,
   ps.sample_order_status_1pxr2,
   YEAR(ps.sample_order_status_1pxr2_time) sample_order_status_1pxr2_year,
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
   ps.sample_status_1sal2,
   YEAR(ps.sample_status_1sal2_time) sample_status_1sal2_year,
   ps.sample_status_1cfd9,
   YEAR(ps.sample_status_1cfd9_time) sample_status_1cfd9_year,
   ps.sample_status_1pxr2,
   YEAR(ps.sample_status_1pxr2_time) sample_status_1pxr2_year,
   ps.num_completed_baseline_ppi_modules,
   ps.num_completed_ppi_modules,
   ps.num_baseline_samples_arrived,
   ps.samples_to_isolate_dna,
   ps.consent_for_cabor,
   YEAR(ps.consent_for_cabor_time) consent_for_cabor_year,
   ps.ehr_status,
   YEAR(ps.ehr_receipt_time) ehr_receipt_year,
   YEAR(ps.ehr_update_time) ehr_update_year,
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


def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass


def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "ehr_receipt",
        sa.Column("ehr_receipt_id", sa.Integer(), nullable=False),
        sa.Column("recorded_time", model.utils.UTCDateTime(), nullable=False),
        sa.Column("received_time", model.utils.UTCDateTime(), nullable=False),
        sa.Column("participant_id", sa.Integer(), nullable=False),
        sa.Column("site_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["participant_id"], ["participant.participant_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["site_id"], ["site.site_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("ehr_receipt_id"),
    )
    op.create_index(op.f("ix_ehr_receipt_recorded_time"), "ehr_receipt", ["recorded_time"], unique=False)
    op.add_column("participant_summary", sa.Column("ehr_receipt_time", model.utils.UTCDateTime(), nullable=True))
    op.add_column("participant_summary", sa.Column("ehr_status", model.utils.Enum(EhrStatus), nullable=True))
    op.add_column("participant_summary", sa.Column("ehr_update_time", model.utils.UTCDateTime(), nullable=True))
    # ### end Alembic commands ###
    op.execute(_PARTICIPANT_VIEW_SQL)
    op.execute(_RAW_PARTICIPANT_VIEW_EXPORT_SQL)
    op.execute(_PPI_PARTICIPANT_VIEW_SQL)


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("participant_summary", "ehr_update_time")
    op.drop_column("participant_summary", "ehr_status")
    op.drop_column("participant_summary", "ehr_receipt_time")
    op.drop_index(op.f("ix_ehr_receipt_recorded_time"), table_name="ehr_receipt")
    op.drop_table("ehr_receipt")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
