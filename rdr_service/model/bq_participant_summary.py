from datetime import datetime
from enum import Enum

from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField


class BQStreetAddressTypeEnum(Enum):
    RESIDENCE = 1
    MAILING = 2
    EMPLOYMENT = 3


# TODO: Revert to using in participant_enum.py when they have been updated to Python 3.7 Enum classes.
class BQModuleStatusEnum(Enum):
    """
    The status of a given questionnaire for this participant.
    Previously named QuestionnaireStatus in participant_enum.py.
    """
    UNSET = 0
    SUBMITTED = 1
    SUBMITTED_NO_CONSENT = 2
    SUBMITTED_NOT_SURE = 3
    SUBMITTED_INVALID = 4


class BQConsentCohort(Enum):
    """
    Which cohort does a participant belong too, based on consent date.
    """
    UNSET = 0
    COHORT_1 = 1  # Beta participants.  Consent before April 24, 2018.
    COHORT_2 = 2  # National Launch Participants. Consent between April 24, 2018 and April 21, 2020 (03:59:59 UTC)
    COHORT_3 = 3  # New Participants with consent starting from April 21, 2020 04:00:00 UTC (midnight eastern)

COHORT_1_CUTOFF = datetime(2018, 4, 24, 0, 0, 0)
COHORT_2_CUTOFF = datetime(2020, 4, 21, 4, 0, 0)


class BQAddressSchema(BQSchema):
    """
    Represents a street address schema.
    Note: Do not use camelCase for property names. Property names must exactly match BQ
          field names.
    """
    addr_type = BQField('addr_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED,
                        fld_enum=BQStreetAddressTypeEnum)
    addr_type_id = BQField('addr_type_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED,
                           fld_enum=BQStreetAddressTypeEnum)
    addr_street_address_1 = BQField('addr_street_address_1', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_street_address_2 = BQField('addr_street_address_2', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_city = BQField('addr_city', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_state = BQField('addr_state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_country = BQField('addr_country', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_zip = BQField('addr_zip', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQModuleStatusSchema(BQSchema):
    """
    Store information about modules submitted
    """
    mod_module = BQField('mod_module', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_baseline_module = BQField('mod_baseline_module', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    mod_authored = BQField('mod_authored', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_created = BQField('mod_created', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_language = BQField('mod_language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_status = BQField('mod_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE, fld_enum=BQModuleStatusEnum)
    mod_status_id = BQField('mod_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                            fld_enum=BQModuleStatusEnum)
    mod_external_id = BQField('mod_external_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_response_status = BQField('mod_response_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_response_status_id = BQField('mod_response_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    mod_questionnaire_response_id = BQField('mod_questionnaire_response_id', BQFieldTypeEnum.INTEGER,
                                            BQFieldModeEnum.NULLABLE)
    mod_consent = BQField('mod_consent', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    mod_consent_value = BQField('mod_consent_value', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_consent_value_id = BQField('mod_consent_value_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    mod_consent_expired = BQField('mod_consent_expired', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_non_participant_answer = BQField('mod_non_participant_answer', BQFieldTypeEnum.STRING,
                                         BQFieldModeEnum.NULLABLE)
    mod_semantic_version = BQField('mod_semantic_version', BQFieldTypeEnum.STRING,
                                         BQFieldModeEnum.NULLABLE)
    mod_irb_mapping = BQField('mod_irb_mapping', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_classification_type = BQField('mod_classification_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mod_classification_type_id = BQField('mod_classification_type_id',
                                         BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQConsentSchema(BQSchema):
    """
    Store participant consent information
    # TODO: Depreciated, ModuleStatusSchema is the replacement.
    """
    consent = BQField('consent', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_id = BQField('consent_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    consent_date = BQField('consent_date', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
    consent_value = BQField('consent_value', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_value_id = BQField('consent_value_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    consent_module = BQField('consent_module', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_module_authored = BQField('consent_module_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    consent_module_created = BQField('consent_module_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    consent_expired = BQField('consent_expired', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_module_external_id = BQField('consent_module_external_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_response_status = BQField('consent_response_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_response_status_id = BQField('consent_response_status_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQRaceSchema(BQSchema):
    """
    Participant race information
    """
    race = BQField('race', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    race_id = BQField('race_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQGenderSchema(BQSchema):
    """
    Participant gender information
    """
    gender = BQField('gender', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gender_id = BQField('gender_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQSexualOrientationSchema(BQSchema):
    """
    Participant race information
    """
    sexual_orientation = BQField('sexual_orientation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sexual_orientation_id = BQField('sexual_orientation_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPhysicalMeasurements(BQSchema):
    """
    Participant Physical Measurements
    """
    pm_status = BQField('pm_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_status_id = BQField('pm_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_created = BQField('pm_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    pm_created_site = BQField('pm_created_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_created_site_id = BQField('pm_created_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_finalized_site = BQField('pm_finalized_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_finalized_site_id = BQField('pm_finalized_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
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

class BQBiobankSampleSchema(BQSchema):
    """
    Biobank sample information
    """
    bbs_test = BQField('bbs_test', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbs_baseline_test = BQField('bbs_baseline_test', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbs_dna_test = BQField('bbs_dna_test', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbs_collected = BQField('bbs_collected', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbs_processed = BQField('bbs_processed', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbs_finalized = BQField('bbs_finalized', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbs_created = BQField('bbs_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbs_confirmed = BQField('bbs_confirmed', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbs_status = BQField('bbs_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbs_status_id = BQField('bbs_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbs_disposed = BQField('bbs_disposed', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbs_disposed_reason = BQField('bbs_disposed_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbs_disposed_reason_id = BQField('bbs_disposed_reason_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbs_biobank_stored_sample_id = BQField('bbs_biobank_stored_sample_id', BQFieldTypeEnum.STRING,
                                           BQFieldModeEnum.NULLABLE)
    bbs_id = BQField('bbs_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbs_hash_id = BQField('bbs_hash_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQBiobankOrderSchema(BQSchema):
    """
    Biobank order information
    """
    bbo_biobank_order_id = BQField('bbo_biobank_order_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_created = BQField('bbo_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbo_status = BQField('bbo_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_status_id = BQField('bbo_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_collected_site = BQField('bbo_collected_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_collected_site_id = BQField('bbo_collected_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_processed_site = BQField('bbo_processed_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_processed_site_id = BQField('bbo_processed_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_finalized_site = BQField('bbo_finalized_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_finalized_site_id = BQField('bbo_finalized_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_samples = BQRecordField('bbo_samples', schema=BQBiobankSampleSchema)
    bbo_tests_ordered = BQField('bbo_tests_ordered', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_tests_stored = BQField('bbo_tests_stored', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    # PDR-243:  Including calculated OrderStatus (UNSET/FINALIZED) and finalized time analogous to the RDR
    # participant_summary.biospecimen_* fields that are based on non-cancelled orders.
    bbo_finalized_time = BQField('bbo_finalized_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    bbo_finalized_status = BQField('bbo_finalized_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_finalized_status_id = BQField('bbo_finalized_status_id',
                                      BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_collection_method = BQField('bbo_collection_method', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bbo_collection_method_id = BQField('bbo_collection_method_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    bbo_id = BQField('bbo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPatientStatusSchema(BQSchema):
    """
    Patient Status History: PatientStatusFlag Enum
    """
    patient_status_created = BQField('patient_status_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    patient_status_modified = BQField('patient_status_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    patient_status_authored = BQField('patient_status_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    patient_status = BQField('patient_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    patient_status_id = BQField('patient_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    hpo = BQField('hpo', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    organization = BQField('organization', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    site = BQField('site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    patient_status_history_id = BQField('patient_status_history_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQEhrReceiptSchema(BQSchema):
    """
    Participant EHR Receipt Histories
    """
    file_timestamp = BQField('file_timestamp', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    first_seen = BQField('first_seen', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    last_seen = BQField('last_seen', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    participant_ehr_receipt_id = BQField('participant_ehr_receipt_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)


class BQPairingHistorySchema(BQSchema):
    """
    Participant pairing history
    """
    last_modified = BQField('last_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    hpo = BQField('hpo', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    organization = BQField('organization', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    site = BQField('site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQParticipantSummarySchema(BQSchema):
    """
    Note: Do not use camelCase for property names. Property names must exactly match BQ
          field names.
    """
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    last_modified = BQField('last_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    participant_id = BQField('participant_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    biobank_id = BQField('biobank_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    research_id = BQField('research_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    participant_origin = BQField('participant_origin', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    first_name = BQField('first_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    middle_name = BQField('middle_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    last_name = BQField('last_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

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
    suspension_status = BQField('suspension_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    suspension_status_id = BQField('suspension_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    suspension_time = BQField('suspension_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    hpo = BQField('hpo', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    organization = BQField('organization', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    site = BQField('site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    distinct_visits = BQField('distinct_visits', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    date_of_birth = BQField('date_of_birth', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
    primary_language = BQField('primary_language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    phone_number = BQField('phone_number', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    login_phone_number = BQField('login_phone_number', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    email = BQField('email', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    addresses = BQRecordField('addresses', schema=BQAddressSchema)

    education = BQField('education', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    education_id = BQField('education_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    income = BQField('income', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    income_id = BQField('income_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    sex = BQField('sex', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_id = BQField('sex_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    sexual_orientation = BQField('sexual_orientation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sexual_orientation_id = BQField('sexual_orientation_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    pm = BQRecordField('pm', schema=BQPhysicalMeasurements)

    races = BQRecordField('races', schema=BQRaceSchema)
    genders = BQRecordField('genders', schema=BQGenderSchema)
    modules = BQRecordField('modules', schema=BQModuleStatusSchema)
    consents = BQRecordField('consents', schema=BQConsentSchema)

    biobank_orders = BQRecordField('biobank_orders', schema=BQBiobankOrderSchema)

    consent_cohort = BQField('consent_cohort', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_cohort_id = BQField('consent_cohort_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    patient_statuses = BQRecordField('patient_statuses', schema=BQPatientStatusSchema)

    test_participant = BQField('test_participant', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    cohort_2_pilot_flag = BQField('cohort_2_pilot_flag', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    cohort_2_pilot_flag_id = BQField('cohort_2_pilot_flag_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biobank_orders = BQRecordField('biobank_orders', schema=BQBiobankOrderSchema)
    # PDR-166:  Additional EHR status / history information enabled by DA-1781
    is_ehr_data_available = BQField('is_ehr_data_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    was_ehr_data_available = BQField('was_ehr_data_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    first_ehr_receipt_time = BQField('first_ehr_receipt_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    latest_ehr_receipt_time = BQField('latest_ehr_receipt_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    ehr_receipts = BQRecordField('ehr_receipts', schema=BQEhrReceiptSchema)

    # PDR-176: Participant Deceased status info
    deceased_authored = BQField('deceased_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    deceased_status = BQField('deceased_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    deceased_status_id = BQField('deceased_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # PDR-178:  Add cabor_authored to align with RDR consent_for_cabor / consent_for_cabor_authored
    cabor_authored = BQField('cabor_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    biobank_id = BQField('biobank_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # PDR-236:  Support for new RDR participant_summary.enrollment_core_minus_pm_time field in PDR data
    enrollment_core_minus_pm = BQField('enrollment_core_minus_pm', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    # PDR-252:  Need to provide AIAN withdrawal ceremony status in PDR data
    withdrawal_aian_ceremony_status = \
        BQField('withdrawal_aian_ceremony_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    withdrawal_aian_ceremony_status_id = \
        BQField('withdrawal_aian_ceremony_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # TODO:  Exclude date of death initially in case it constitutes PII, determine if it is needed in PDR. Add to
    # end of field list if enabled later
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

    ubr_sex = BQField('ubr_sex', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_sexual_orientation = BQField('ubr_sexual_orientation', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_gender_identity = BQField('ubr_gender_identity', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_ethnicity = BQField('ubr_ethnicity', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_geography = BQField('ubr_geography', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_education = BQField('ubr_education', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_income = BQField('ubr_income', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_sexual_gender_minority = BQField('ubr_sexual_gender_minority', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    ubr_disability = BQField('ubr_disability', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_age_at_consent = BQField('ubr_age_at_consent', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_overall = BQField('ubr_overall', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    sexual_orientations = BQRecordField('sexual_orientations', schema=BQSexualOrientationSchema)
    age_at_consent = BQField('age_at_consent', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    research_id = BQField('research_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    # New Goal 1 additions, ingested from RDR
    # enrollment_core_minus_pm is the legacy (pre V3.0) timestamp from RDR participant_summary
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


class BQParticipantSummary(BQTable):
    """ Participant Summary BigQuery Table """
    __tablename__ = 'participant_summary'
    __schema__ = BQParticipantSummarySchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('all-of-us-rdr-prod', None)),  # Block participant summary from production project.
        ('all-of-us-rdr-stable', ('aou-pdr-data-stable', 'rdr_ops_data_view')),
        ('pmi-drc-api-test', ('aou-pdr-data-test', 'rdr_ops_data_view')),
    ]


class BQParticipantSummaryView(BQView):
    __viewname__ = 'v_participant_summary'
    __viewdescr__ = 'Participant Summary View'
    __table__ = BQParticipantSummary
