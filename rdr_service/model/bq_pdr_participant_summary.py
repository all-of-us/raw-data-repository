#
# BigQuery schemas for PDR that do not contain PII.
#
#
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField
from rdr_service.model.bq_participant_summary import (
    BQRaceSchema,
    BQGenderSchema,
    BQModuleStatusSchema,
    BQConsentSchema,
    BQPatientStatusSchema,
    BQBiobankOrderSchema,
    BQPairingHistorySchema,
    BQSexualOrientationSchema
)


class BQPDRPhysicalMeasurements(BQSchema):
    """
    PDR Participant Physical Measurements
    """
    pm_status = BQField('pm_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_status_id = BQField('pm_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_finalized = BQField('pm_finalized', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    pm_physical_measurements_id = BQField('pm_physical_measurements_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    pm_amended_measurements_id = BQField('pm_amended_measurements_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    pm_final = BQField('pm_final', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_restored = BQField('pm_restored', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_questionnaire_response_id = BQField('pm_questionnaire_response_id',
                                           BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_collect_type = BQField('pm_collect_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_collect_type_id = BQField('pm_collect_type_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_origin = BQField('pm_origin', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_origin_measurement_unit = BQField('pm_origin_measurement_unit', BQFieldTypeEnum.STRING,
                                         BQFieldModeEnum.NULLABLE)
    pm_origin_measurement_unit_id = BQField('pm_origin_measurement_unit_id', BQFieldTypeEnum.INTEGER,
                                            BQFieldModeEnum.NULLABLE)



# TODO:  Deprecate use of this class and add these fields to the BQBiobankOrderSchema
class BQPDRBiospecimenSchema(BQSchema):
    """
    PDR Summary of Biobank Orders and Tests
    """
    biosp_status = BQField('biosp_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    biosp_status_id = BQField('biosp_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biosp_order_time = BQField('biosp_order_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    biosp_isolate_dna = BQField('biosp_isolate_dna', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biosp_isolate_dna_confirmed = BQField('biosp_isolate_dna_confirmed',
                                          BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biosp_baseline_tests = BQField('biosp_baseline_tests', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biosp_baseline_tests_confirmed = BQField('biosp_baseline_tests_confirmed',
                                             BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPDREhrReceiptSchema(BQSchema):
    """
    PDR Participant EHR Receipt Histories
    """
    file_timestamp = BQField('file_timestamp', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    first_seen = BQField('first_seen', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    last_seen = BQField('last_seen', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    participant_ehr_receipt_id = BQField('participant_ehr_receipt_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)


class BQPDRParticipantSummarySchema(BQSchema):
    """
    A subset of participant summary for PDR that does not contain PII.
    Note: !!! If you add fields here, remember to add them to the View as well. !!!
    Note: Do not use camelCase for property names. Property names must exactly match BQ
          field names.
    """
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    participant_id = BQField('participant_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    participant_origin = BQField('participant_origin', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    addr_state = BQField('addr_state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_zip = BQField('addr_zip', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    is_ghost_id = BQField('is_ghost_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

    sign_up_time = BQField('sign_up_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status = BQField('enrollment_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enrollment_status_id = BQField('enrollment_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    enrollment_member = BQField('enrollment_member', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_core_ordered = BQField('enrollment_core_ordered', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_core_stored = BQField('enrollment_core_stored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    # PDR-106: The EHR fields are needed in PDR by PTSC and for consistency should come from RDR vs. Curation BigQuery
    ehr_status = BQField('ehr_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    ehr_status_id = BQField('ehr_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ehr_receipt = BQField('ehr_receipt', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    ehr_update = BQField('ehr_update', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    withdrawal_status = BQField('withdrawal_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    withdrawal_status_id = BQField('withdrawal_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    withdrawal_time = BQField('withdrawal_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    withdrawal_authored = BQField('withdrawal_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    withdrawal_reason = BQField('withdrawal_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    withdrawal_reason_id = BQField('withdrawal_reason_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    withdrawal_reason_justification = BQField('withdrawal_reason_justification', BQFieldTypeEnum.STRING,
                                              BQFieldModeEnum.NULLABLE)

    hpo = BQField('hpo', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    organization = BQField('organization', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    site = BQField('site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    date_of_birth = BQField('date_of_birth', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
    primary_language = BQField('primary_language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    education = BQField('education', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    education_id = BQField('education_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    income = BQField('income', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    income_id = BQField('income_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    sex = BQField('sex', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_id = BQField('sex_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    pm = BQRecordField('pm', schema=BQPDRPhysicalMeasurements)

    races = BQRecordField('races', schema=BQRaceSchema)
    genders = BQRecordField('genders', schema=BQGenderSchema)
    sexual_orientations = BQRecordField('sexual_orientations', schema=BQSexualOrientationSchema)
    modules = BQRecordField('modules', schema=BQModuleStatusSchema)
    consents = BQRecordField('consents', schema=BQConsentSchema)

    biospec = BQRecordField('biospec', schema=BQPDRBiospecimenSchema)

    ubr_sex = BQField('ubr_sex', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_sexual_orientation = BQField('ubr_sexual_orientation', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_gender_identity = BQField('ubr_gender_identity', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_ethnicity = BQField('ubr_ethnicity', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_geography = BQField('ubr_geography', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_education = BQField('ubr_education', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_income = BQField('ubr_income', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_sexual_gender_minority = BQField('ubr_sexual_gender_minority', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    ubr_overall = BQField('ubr_overall', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_age_at_consent = BQField('ubr_age_at_consent', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    consent_cohort = BQField('consent_cohort', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_cohort_id = BQField('consent_cohort_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    email_available = BQField('email_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    phone_number_available = BQField('phone_number_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    ubr_disability = BQField('ubr_disability', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    patient_statuses = BQRecordField('patient_statuses', schema=BQPatientStatusSchema)

    test_participant = BQField('test_participant', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    suspension_status = BQField('suspension_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    suspension_status_id = BQField('suspension_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    suspension_time = BQField('suspension_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    cohort_2_pilot_flag = BQField('cohort_2_pilot_flag', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    cohort_2_pilot_flag_id = BQField('cohort_2_pilot_flag_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biobank_orders = BQRecordField('biobank_orders', schema=BQBiobankOrderSchema)
    # PDR-166:  Additional EHR status / history information enabled by DA-1781
    is_ehr_data_available = BQField('is_ehr_data_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    was_ehr_data_available = BQField('was_ehr_data_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    first_ehr_receipt_time = BQField('first_ehr_receipt_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    latest_ehr_receipt_time = BQField('latest_ehr_receipt_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    ehr_receipts = BQRecordField('ehr_receipts', schema=BQPDREhrReceiptSchema)

    # PDR-176: Participant deceased status info
    deceased_authored = BQField('deceased_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    deceased_status = BQField('deceased_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    deceased_status_id = BQField('deceased_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # PDR-178:  CABoR details.  This is part of ConsentPII, but for various reasons the easiest way to align with
    # RDR CABoR tracking is to surface the appropriate authored date here.  Presence of a date (vs. null/None also
    # acts as the true/false flag equivalent to RDR participant_summary.consent_for_cabor field
    cabor_authored = BQField('cabor_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    biobank_id = BQField('biobank_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # PDR-236:  Support for new RDR participant_summary.enrollment_core_minus_pm_time field in PDR data
    enrollment_core_minus_pm = BQField('enrollment_core_minus_pm', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    # PDR-252:  Need to provide AIAN withdrawal ceremony status
    withdrawal_aian_ceremony_status = \
        BQField('withdrawal_aian_ceremony_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    withdrawal_aian_ceremony_status_id = \
        BQField('withdrawal_aian_ceremony_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # TODO:  Exclude date of death initially in case it constitutes PII.  Add to end of field list if it is
    # enabled later
    # date_of_death = BQField('date_of_death', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)

    enrl_registered_time = BQField('enrl_registered_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrl_participant_time = BQField('enrl_participant_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrl_participant_plus_ehr_time = BQField('enrl_participant_plus_ehr_time', BQFieldTypeEnum.DATETIME,
                                           BQFieldModeEnum.NULLABLE)
    enrl_core_participant_minus_pm_time = BQField('enrl_core_participant_minus_pm_time', BQFieldTypeEnum.DATETIME,
                                                BQFieldModeEnum.NULLABLE)
    enrl_core_participant_time = BQField('enrl_core_participant_time', BQFieldTypeEnum.DATETIME,
                                         BQFieldModeEnum.NULLABLE)
    pairing_history = BQRecordField('pairing_history', schema=BQPairingHistorySchema)
    enrl_status = BQField('enrl_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enrl_status_id = BQField('enrl_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    age_at_consent = BQField('age_at_consent', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    research_id = BQField('research_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # New Goal 1 additions, ingested from RDR
    enrollment_status_legacy_v2 = BQField('enrollment_status_legacy_v2', BQFieldTypeEnum.STRING,
                                          BQFieldModeEnum.NULLABLE)
    enrollment_status_legacy_v2_id = BQField('enrollment_status_legacy_v2_id', BQFieldTypeEnum.INTEGER,
                                             BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0 = BQField('enrollment_status_v3_0', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0_id = BQField('enrollment_status_v3_0_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0_participant_time = BQField('enrollment_status_v3_0_participant_time',
                                                      BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0_participant_plus_ehr_time = BQField('enrollment_status_v3_0_participant_plus_ehr_time',
                                                               BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0_pmb_eligible_time = BQField('enrollment_status_v3_0_pmb_eligible_time',
                                                       BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0_core_minus_pm_time = BQField('enrollment_status_v3_0_core_minus_pm_time',
                                                        BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_0_core_time = BQField('enrollment_status_v3_0_core_time', BQFieldTypeEnum.DATETIME,
                                               BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1 = BQField('enrollment_status_v3_1', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_id = BQField('enrollment_status_v3_1_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_participant_time = BQField('enrollment_status_v3_1_participant_time',
                                                      BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_participant_plus_ehr_time = BQField('enrollment_status_v3_1_participant_plus_ehr_time',
                                                               BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_participant_plus_basics_time = BQField('enrollment_status_v3_1_participant_plus_basics_time',
                                                               BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_core_minus_pm_time = BQField('enrollment_status_v3_1_core_minus_pm_time',
                                                        BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_core_time = BQField('enrollment_status_v3_1_core_time', BQFieldTypeEnum.DATETIME,
                                               BQFieldModeEnum.NULLABLE)
    enrollment_status_v3_1_participant_plus_baseline_time = \
        BQField('enrollment_status_v3_1_participant_plus_baseline_time', BQFieldTypeEnum.DATETIME,
                BQFieldModeEnum.NULLABLE)
    health_datastream_sharing_status_v3_1 = BQField('health_datastream_sharing_status_v3_1',
                                                    BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    health_datastream_sharing_status_v3_1_id = BQField('health_datastream_sharing_status_v3_1_id',
                                                       BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPDRParticipantSummary(BQTable):
    """ PDR Participant Summary BigQuery Table """
    __tablename__ = 'pdr_participant'
    __schema__ = BQPDRParticipantSummarySchema


class BQPDRParticipantSummaryView(BQView):
    """ PDR Team view of the Participant Summary """
    __viewname__ = 'v_pdr_participant'
    __viewdescr__ = 'PDR Participant Summary View'
    __table__ = BQPDRParticipantSummary
    __pk_id__ = 'participant_id'
    # We need to build a SQL statement with all fields except sub-tables and remove duplicates.
    __sql__ = """
        SELECT
          %%FIELD_NAMES%%
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
              FROM `{project}`.{dataset}.pdr_participant
          ) ps
          WHERE ps.rn = 1 and ps.withdrawal_status_id = 1 and ps.test_participant != 1
      """.replace('%%FIELD_NAMES%%', BQPDRParticipantSummarySchema.get_sql_field_names(
        exclude_fields=[
            'pm',
            'genders',
            'races',
            'sexual_orientations',
            'modules',
            'consents',
            'biospec',
            'patient_statuses',
            'biobank_orders',
            'ehr_receipts',
            'pairing_history'
        ])
    )


class BQPDRParticipantSummaryAllView(BQPDRParticipantSummaryView):
    __viewname__ = 'v_pdr_participant_all'
    __viewdescr__ = 'PDR Participant Summary All View'
    __sql__ = """
            SELECT
              %%FIELD_NAMES%%
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
                  FROM `{project}`.{dataset}.pdr_participant
              ) ps
              WHERE ps.rn = 1
    """.replace('%%FIELD_NAMES%%', BQPDRParticipantSummarySchema.get_sql_field_names(
        exclude_fields=[
            'pm',
            'genders',
            'races',
            'sexual_orientations',
            'modules',
            'consents',
            'biospec',
            'patient_statuses',
            'biobank_orders',
            'ehr_receipts',
            'pairing_history'
        ])
    )


# TODO:  This is now a custom view in PDR BigQuery (as of PDR-262).  Needs to be disabled here so it will not be
# updated by migrate-bq tool.   Consider moving all custom views into our model?  Some (like this one) will have
# extremely complicated SQL definitions, so unclear if that is a viable/best solution
# class BQPDRParticipantSummaryWithdrawnView(BQView):
#   __viewname__ = 'v_pdr_participant_withdrawn'
#   __viewdescr__ = 'PDR Participant Summary Withdrawn View'
#   __table__ = BQPDRParticipantSummary
#   __sql__ = BQPDRParticipantSummaryView.__sql__.replace('ps.withdrawal_status_id = 1',
#                                                         'ps.withdrawal_status_id != 1')


class BQPDRPMView(BQView):
    __viewname__ = 'v_pdr_participant_pm'
    __viewdescr__ = 'PDR Physical Measurements View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(pm) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRGenderView(BQView):
    __viewname__ = 'v_pdr_participant_gender'
    __viewdescr__ = 'PDR Participant Gender View'
    __table__ = BQPDRParticipantSummary
    __pk_id__ = 'participant_id'
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(genders) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRRaceView(BQView):
    __viewname__ = 'v_pdr_participant_race'
    __viewdescr__ = 'PDR Participant Race View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(races) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRSexualOrientationView(BQView):
    __viewname__ = 'v_pdr_participant_sexual_orientation'
    __viewdescr__ = 'PDR Participant Sexual Orientation View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(sexual_orientations) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRModuleView(BQView):
    __viewname__ = 'v_pdr_participant_module'
    __viewdescr__ = 'PDR Participant Survey Module View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id,
           nt.mod_module, nt.mod_baseline_module,
           CAST(nt.mod_authored AS DATETIME) as mod_authored, CAST(nt.mod_created AS DATETIME) as mod_created,
           nt.mod_language, nt.mod_status, nt.mod_status_id, nt.mod_response_status, nt.mod_response_status_id,
           nt.mod_external_id, nt.mod_questionnaire_response_id, nt.mod_consent, nt.mod_consent_value,
           nt.mod_consent_value_id, nt.mod_consent_expired, nt.mod_non_participant_answer,
           nt.mod_classification_type, nt.mod_classification_type_id
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(modules) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRConsentView(BQView):
    __viewname__ = 'v_pdr_participant_consent'
    __viewdescr__ = 'PDR Participant Consent View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(consents) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRBioSpecView(BQView):
    __viewname__ = 'v_pdr_biospec'
    __viewdescr__ = 'PDR Participant BioBank Order View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(biospec) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRPatientStatuesView(BQView):
    __viewname__ = 'v_pdr_participant_patient_status'
    __viewdescr__ = 'PDR Participant Patient Status View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(patient_statuses) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """


class BQPDRParticipantBiobankOrderView(BQView):
    __viewname__ = 'v_pdr_participant_biobank_order'
    __viewdescr__ = 'PDR Participant Biobank Order Details view'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
      SELECT ps.id, ps.created, ps.modified, ps.participant_id,
             nt.bbo_biobank_order_id,
             nt.bbo_created,
             nt.bbo_status,
             nt.bbo_status_id,
             nt.bbo_collected_site,
             nt.bbo_collected_site_id,
             nt.bbo_processed_site,
             nt.bbo_processed_site_id,
             nt.bbo_finalized_site,
             nt.bbo_finalized_site_id,
             nt.bbo_finalized_time,
             nt.bbo_finalized_status,
             nt.bbo_finalized_status_id,
             nt.bbo_tests_ordered,
             nt.bbo_tests_stored,
             nt.bbo_collection_method,
             nt.bbo_collection_method_id,
             nt.bbo_id
        FROM (
          SELECT *,
              ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
            FROM `{project}`.{dataset}.pdr_participant
        ) ps cross join unnest(biobank_orders) as nt
        WHERE ps.rn = 1 and ps.test_participant != 1
    """


class BQPDRParticipantBiobankSampleView(BQView):
    __viewname__ = 'v_pdr_participant_biobank_sample'
    __viewdescr__ = 'PDR Participant Biobank Sample Details view'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
         SELECT ps.id, ps.created, ps.modified, ps.participant_id,
               bbo.bbo_biobank_order_id,
               nt.bbs_test,
               nt.bbs_baseline_test,
               nt.bbs_dna_test,
               nt.bbs_collected,
               nt.bbs_processed,
               nt.bbs_finalized,
               nt.bbs_created,
               nt.bbs_confirmed,
               nt.bbs_status,
               nt.bbs_status_id,
               nt.bbs_disposed,
               nt.bbs_disposed_reason,
               nt.bbs_disposed_reason_id,
               nt.bbs_biobank_stored_sample_id,
               nt.bbs_id,
               nt.bbs_hash_id
           FROM (
              SELECT *,
                  ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
                FROM `{project}`.{dataset}.pdr_participant
            ) ps cross join unnest(biobank_orders) as bbo, unnest(bbo.bbo_samples) as nt
            WHERE ps.rn = 1 and ps.test_participant != 1
    """


class BQPDREhrReceiptView(BQView):
    __viewname__ = 'v_pdr_participant_ehr_receipt'
    __viewdescr__ = 'PDR Participant EHR Receipts View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(ehr_receipts) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """

class BQPDRPairingHistoryView(BQView):
    __viewname__ = 'v_pdr_participant_pairing_history'
    __viewdescr__ = 'PDR Participant Pairing History View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY modified desc, test_participant desc) AS rn
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(pairing_history) as nt
      WHERE ps.rn = 1 and ps.test_participant != 1
  """
