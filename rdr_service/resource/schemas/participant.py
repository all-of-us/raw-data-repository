#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import date
from enum import Enum
from marshmallow import validate

from rdr_service.participant_enums import (QuestionnaireStatus, ParticipantCohort, Race, GenderIdentity,
    PhysicalMeasurementsStatus, OrderStatus, EnrollmentStatusV2, EhrStatus, WithdrawalStatus, WithdrawalReason,
    SuspensionStatus, QuestionnaireResponseStatus, QuestionnaireResponseClassificationType,
    DeceasedStatus, ParticipantCohortPilotFlag, WithdrawalAIANCeremonyStatus, BiobankOrderStatus,
    SampleCollectionMethod, PhysicalMeasurementsCollectType, OriginMeasurementUnit,
    EnrollmentStatusV30, EnrollmentStatusV31, DigitalHealthSharingStatusV31)
from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID


class SexualOrientationEnum(Enum):
    SexualOrientation_None = 1
    SexualOrientation_Straight = 2
    SexualOrientation_Lesbian = 3
    SexualOrientation_Gay = 4
    SexualOrientation_Bisexual = 5


class StreetAddressTypeEnum(Enum):
    RESIDENCE = 1
    MAILING = 2
    EMPLOYMENT = 3


COHORT_1_CUTOFF = date(2018, 4, 24)
COHORT_2_CUTOFF = date(2020, 4, 16)


BIOBANK_UNIQUE_TEST_IDS = {
    # The value for each test must be preserved, only append new tests to the end.
    "1CFD9": '01',
    "1ED04": '02',
    "1ED10": '03',
    "1HEP4": '04',
    "1PST8": '05',
    "1PXR2": '06',
    "1SAL": '07',
    "1SAL2": '08',
    "1SST8": '09',
    "1UR10": '10',
    "2ED10": '11',
    "2PST8": '12',
    "2SST8": '13',
    "1ED02": '14',
    "1PS08": '15',
}


class AddressSchema(Schema):
    """ Represents a street address """
    addr_type = fields.EnumString(enum=StreetAddressTypeEnum)
    addr_type_id = fields.EnumInteger(enum=StreetAddressTypeEnum)
    addr_street_address_1 = fields.String(validate=validate.Length(max=255))
    addr_street_address_2 = fields.String(validate=validate.Length(max=255))
    addr_city = fields.String(validate=validate.Length(max=255))
    addr_state = fields.String(validate=validate.Length(max=2))
    addr_country = fields.String(validate=validate.Length(max=255))
    addr_zip = fields.String(validate=validate.Length(max=10))

    class Meta:
        schema_id = SchemaID.participant_address
        resource_uri = 'Participant/{participant_id}/Address'
        # PII: remove all fields except 'addr_state' and 'addr_zip'
        pii_fields = ('addr_type', 'addr_type_id', 'addr_street_address_1', 'addr_street_address_2', 'addr_country')
        # PII: strip last two digits from zip code.
        pii_filter = {
            'addr_zip': (lambda v: str(v).strip()[:3] if v else None)
        }


class ModuleStatusSchema(Schema):
    """ Store information about modules submitted """
    module = fields.String(validate=validate.Length(max=80))
    baseline_module = fields.Boolean()
    module_authored = fields.DateTime()
    module_created = fields.DateTime()
    language = fields.String(validate=validate.Length(max=2))
    status = fields.EnumString(enum=QuestionnaireStatus)
    status_id = fields.EnumInteger(enum=QuestionnaireStatus)
    external_id = fields.String(validate=validate.Length(max=120))
    response_status = fields.EnumString(enum=QuestionnaireResponseStatus)
    response_status_id = fields.EnumInteger(enum=QuestionnaireResponseStatus)
    questionnaire_response_id = fields.Int32()
    consent = fields.Boolean()
    consent_value = fields.String(validate=validate.Length(max=80))
    consent_value_id = fields.Int32()
    consent_expired = fields.String(validate=validate.Length(max=80))
    non_participant_answer = fields.String(validate=validate.Length(max=60))
    semantic_version = fields.String(validate=validate.Length(max=100))
    irb_mapping = fields.String(validate=validate.Length(max=500))
    classification_type = fields.EnumString(enum=QuestionnaireResponseClassificationType)
    classification_type_id = fields.EnumInteger(enum=QuestionnaireResponseClassificationType)

    class Meta:
        schema_id = SchemaID.participant_modules
        resource_uri = 'Participant/{participant_id}/Modules'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).

# TODO: Deprecated, but leave around until BigQuery support is removed.
# class ConsentSchema(Schema):
#     """ Store participant consent information """
#     consent = fields.String(validate=validate.Length(max=80))
#     consent_id = fields.Int32()
#     consent_date = fields.Date()
#     consent_value = fields.String(validate=validate.Length(max=80))
#     consent_value_id = fields.Int32()
#     consent_module = fields.String(validate=validate.Length(max=80))
#     consent_module_authored = fields.DateTime()
#     consent_module_created = fields.DateTime()
#     consent_expired = fields.String(validate=validate.Length(max=80))
#     consent_module_external_id = fields.String(validate=validate.Length(max=120))
#     consent_response_status = fields.EnumString(enum=QuestionnaireResponseStatus)
#     consent_response_status_id = fields.EnumInteger(enum=QuestionnaireResponseStatus)
#
#     class Meta:
#         schema_id = SchemaID.participant_consents
#         resource_uri = 'Participant/{participant_id}/Consents'
#         # Exclude fields and/or functions to strip PII information from fields.
#         pii_fields = ()  # List fields that contain PII data.
#         pii_filter = {}  # dict(field: lambda function).


class RaceSchema(Schema):
    """ Participant race information """
    race = fields.EnumString(enum=Race)
    race_id = fields.EnumInteger(enum=Race)

    class Meta:
        schema_id = SchemaID.participant_race
        resource_uri = 'Participant/{participant_id}/Races'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenderSchema(Schema):
    """ Participant gender information """
    gender = fields.EnumString(GenderIdentity)
    gender_id = fields.EnumInteger(GenderIdentity)

    class Meta:
        schema_id = SchemaID.participant_gender
        resource_uri = 'Participant/{participant_id}/Genders'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class SexualOrientationSchema(Schema):
    """ Participant race information """
    sexual_orientation = fields.EnumString(enum=SexualOrientationEnum)
    sexual_orientation_id = fields.EnumInteger(enum=SexualOrientationEnum)

    class Meta:
        schema_id = SchemaID.participant_sexual_orientation
        resource_uri = 'Participant/{participant_id}/SexualOrientations'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class PhysicalMeasurementsSchema(Schema):
    """ Participant Physical Measurements """
    physical_measurements_id = fields.Int32()
    questionnaire_response_id = fields.Int32()
    status = fields.EnumString(enum=PhysicalMeasurementsStatus)
    status_id = fields.EnumInteger(enum=PhysicalMeasurementsStatus)
    created = fields.DateTime()
    created_site = fields.String(validate=validate.Length(max=255))
    created_site_id = fields.Int32()
    final = fields.Boolean()
    finalized_site = fields.String(validate=validate.Length(max=255))
    finalized_site_id = fields.Int32()
    finalized = fields.DateTime()
    amended_measurements_id = fields.Int32()
    collect_type = fields.EnumString(enum=PhysicalMeasurementsCollectType)
    collect_type_id = fields.EnumInteger(enum=PhysicalMeasurementsCollectType)
    origin = fields.String(validate=validate.Length(max=255))
    origin_measurement_unit = fields.EnumString(enum=OriginMeasurementUnit)
    origin_measurement_unit_id = fields.EnumInteger(enum=OriginMeasurementUnit)
    restored = fields.Boolean()

    class Meta:
        schema_id = SchemaID.participant_physical_measurements
        resource_uri = 'Participant/{participant_id}/PhysicalMeasurements'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class BiobankSampleSchema(Schema):
    """ Biobank sample information """
    id = fields.Int64()
    hash_id = fields.Int32()
    test = fields.String(validate=validate.Length(max=80))
    baseline_test = fields.Boolean()
    dna_test = fields.Boolean()
    collected = fields.DateTime()
    processed = fields.DateTime()
    finalized = fields.DateTime()
    created = fields.DateTime()
    confirmed = fields.DateTime()
    status = fields.String(validate=validate.Length(max=50))
    status_id = fields.Int32()
    disposed = fields.DateTime()
    disposed_reason = fields.String(validate=validate.Length(max=50))
    disposed_reason_id = fields.Int32()
    biobank_stored_sample_id = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.participant_biobank_order_samples
        resource_uri = 'Participant/{participant_id}/BiobankOrders/{biobank_order_id}/BiobankSamples'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class BiobankOrderSchema(Schema):
    """
    Biobank order information
    """
    id = fields.Int32()
    biobank_order_id = fields.String(validate=validate.Length(max=80))
    created = fields.DateTime()
    status = fields.EnumString(enum=BiobankOrderStatus)
    status_id = fields.EnumInteger(enum=BiobankOrderStatus)
    collection_method = fields.EnumString(enum=SampleCollectionMethod,
        description='Collection method name used for biobank order.')
    collection_method_id = fields.EnumInteger(enum=SampleCollectionMethod,
        description='Collection method id value used for biobank order.')
    collected_site = fields.String(validate=validate.Length(max=255))
    collected_site_id = fields.Int32()
    processed_site = fields.String(validate=validate.Length(max=255))
    processed_site_id = fields.Int32()
    finalized_site = fields.String(validate=validate.Length(max=255))
    finalized_site_id = fields.Int32()
    # PDR-243:  Including calculated OrderStatus (UNSET/FINALIZED) and finalized time analogous to the RDR
    # participant_summary.biospecimen_* fields that are based on non-cancelled orders.
    finalized_time = fields.DateTime()
    finalized_status = fields.EnumString(enum=OrderStatus)
    finalized_status_id = fields.EnumInteger(enum=OrderStatus)
    samples = fields.Nested(BiobankSampleSchema, many=True)
    tests_ordered = fields.Int32()
    tests_stored = fields.Int32()

    class Meta:
        schema_id = SchemaID.participant_biobank_orders
        resource_uri = 'Participant/{participant_id}/BiobankOrders'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class PatientStatusSchema(Schema):
    """
    Patient Status History: PatientStatusFlag Enum
    """
    patient_status_history_id = fields.Int32()
    patient_status_created = fields.DateTime()
    patient_status_modified = fields.DateTime()
    patient_status_authored = fields.DateTime()
    patient_status = fields.String(validate=validate.Length(max=20))
    patient_status_id = fields.Int32()
    hpo = fields.String(validate=validate.Length(max=20))
    hpo_id = fields.Int32()
    organization = fields.String(validate=validate.Length(max=255))
    organization_id = fields.Int32()
    site = fields.String(validate=validate.Length(max=255))
    site_id = fields.Int32()
    comment = fields.Text()
    user = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.patient_statuses
        resource_uri = 'Participant/{participant_id}/PatientStatuses'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ('comment', 'user')
        pii_filter = {}  # dict(field: lambda function).


class EhrReceiptSchema(Schema):
    """
    Participant EHR status records.
    """
    # TODO:  Confirm if this should be the resource_pk_id in the Meta data?
    participant_ehr_receipt_id = fields.Int32()
    file_timestamp = fields.DateTime()
    first_seen = fields.DateTime()
    last_seen = fields.DateTime()

    class Meta:
        schema_id = SchemaID.ehr_recept
        resource_uri = 'Participant/{participant_id}/EHRReceipt'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class PairingHistorySchema(Schema):
    """
    Participant pairing history
    """
    last_modified = fields.DateTime(
        description='Last time the participant pairing was updated.')
    hpo = fields.String(validate=validate.Length(max=20))
    hpo_id = fields.Int32()
    organization = fields.String(validate=validate.Length(max=255))
    organization_id = fields.Int32()
    site = fields.String(validate=validate.Length(max=255))
    site_id = fields.Int32()

    class Meta:
        schema_id = SchemaID.participant_pairing_history
        resource_uri = 'Participant/{participant_id}/PairingHistory'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class ParticipantSchema(Schema):
    """ Participant Activity Summary Schema """
    last_modified = fields.DateTime(
        description='Last time the participant record was updated.')

    participant_id = fields.String(validate=validate.Length(max=10), required=True)
    biobank_id = fields.Int32()
    research_id = fields.Int32()
    participant_origin = fields.String(validate=validate.Length(max=60))

    first_name = fields.String(validate=validate.Length(max=255))
    middle_name = fields.String(validate=validate.Length(max=255))
    last_name = fields.String(validate=validate.Length(max=255))

    is_ghost_id = fields.Boolean()
    test_participant = fields.Boolean()

    sign_up_time = fields.DateTime()
    age_at_consent = fields.Int16()
    enrl_status = fields.String(validate=validate.Length(max=40))
    enrl_status_id = fields.Int32()
    enrl_registered_time = fields.DateTime()
    enrl_participant_time = fields.DateTime()
    enrl_participant_plus_ehr_time = fields.DateTime()
    enrl_core_participant_minus_pm_time = fields.DateTime()
    enrl_core_participant_time = fields.DateTime()

    # Retaining during Goal 1 transition;  may be deprecated after V30/V31 acceptance
    enrollment_status = fields.EnumString(enum=EnrollmentStatusV2)
    enrollment_status_id = fields.EnumInteger(enum=EnrollmentStatusV2)
    enrollment_member = fields.DateTime()
    enrollment_core_ordered = fields.DateTime()
    enrollment_core_stored = fields.DateTime()
    enrollment_core_minus_pm = fields.DateTime()

    # Goal 1 new additions
    # TODO:  The v2 fields are temporary to do consistency checks for PEO report vs. RDR values
    enrollment_status_legacy_v2 = fields.EnumString(enum=EnrollmentStatusV2)
    enrollment_status_legacy_v2_id = fields.EnumInteger(enum=EnrollmentStatusV2)
    enrollment_status_v3_0 = fields.EnumString(enum=EnrollmentStatusV30)
    enrollment_status_v3_0_id = fields.EnumInteger(enum=EnrollmentStatusV30)
    enrollment_status_v3_0_participant_time = fields.DateTime()
    enrollment_status_v3_0_participant_plus_ehr_time = fields.DateTime()
    enrollment_status_v3_0_pmb_eligible_time = fields.DateTime()
    enrollment_status_v3_0_core_minus_pm_time = fields.DateTime()
    enrollment_status_v3_0_core_time = fields.DateTime()

    enrollment_status_v3_1 = fields.EnumString(enum=EnrollmentStatusV31)
    enrollment_status_v3_1_id = fields.EnumInteger(enum=EnrollmentStatusV31)
    enrollment_status_v3_1_participant_time = fields.DateTime()
    enrollment_status_v3_1_participant_plus_ehr_time = fields.DateTime()
    enrollment_status_v3_1_participant_plus_basics_time = fields.DateTime()
    enrollment_status_v3_1_core_minus_pm_time = fields.DateTime()
    enrollment_status_v3_1_core_time = fields.DateTime()
    enrollment_status_v3_1_participant_plus_baseline_time = fields.DateTime()
    health_datastream_sharing_status_v3_1 = fields.EnumString(enum=DigitalHealthSharingStatusV31)
    health_datastream_sharing_status_v3_1_id = fields.EnumInteger(enum=DigitalHealthSharingStatusV31)
    # These EHR fields are populated from Curation data.
    ehr_status = fields.EnumString(enum=EhrStatus)
    ehr_status_id = fields.EnumInteger(enum=EhrStatus)
    ehr_receipt = fields.DateTime()
    ehr_update = fields.DateTime()

    withdrawal_status = fields.EnumString(enum=WithdrawalStatus)
    withdrawal_status_id = fields.EnumInteger(enum=WithdrawalStatus)
    withdrawal_time = fields.DateTime()
    withdrawal_authored = fields.DateTime()
    withdrawal_reason = fields.EnumString(enum=WithdrawalReason)
    withdrawal_reason_id = fields.EnumInteger(enum=WithdrawalReason)
    withdrawal_reason_justification = fields.Text()
    # PDR-252:  Must include AIAN ceremony decision in PDR data
    withdrawal_aian_ceremony_status = fields.EnumString(enum=WithdrawalAIANCeremonyStatus)
    withdrawal_aian_ceremony_status_id = fields.EnumInteger(enum=WithdrawalAIANCeremonyStatus)
    suspension_status = fields.EnumString(enum=SuspensionStatus)
    suspension_status_id = fields.EnumInteger(enum=SuspensionStatus)
    suspension_time = fields.DateTime()

    hpo = fields.String(validate=validate.Length(max=20))
    hpo_id = fields.Int32()
    organization = fields.String(validate=validate.Length(max=255))
    organization_id = fields.Int32()
    site = fields.String(validate=validate.Length(max=255))
    site_id = fields.Int32()

    distinct_visits = fields.Int32()

    date_of_birth = fields.Date()
    primary_language = fields.String(validate=validate.Length(max=80))

    phone_number = fields.String(validate=validate.Length(max=80))
    login_phone_number = fields.String(validate=validate.Length(max=80))
    email = fields.String(validate=validate.Length(max=255))
    phone_number_available = fields.Boolean()
    email_available = fields.Boolean()

    addresses = fields.Nested(AddressSchema, many=True)

    education = fields.String(validate=validate.Length(max=80))
    education_id = fields.Int32()
    income = fields.String(validate=validate.Length(max=80))
    income_id = fields.Int32()

    sex = fields.String(validate=validate.Length(max=80))
    sex_id = fields.Int32()

    pairing_history = fields.Nested(PairingHistorySchema, many=True)
    pm = fields.Nested(PhysicalMeasurementsSchema, many=True)

    races = fields.Nested(RaceSchema, many=True)
    genders = fields.Nested(GenderSchema, many=True)
    sexual_orientations = fields.Nested(SexualOrientationSchema, many=True)
    modules = fields.Nested(ModuleStatusSchema, many=True)
    # TODO: Deprecated, but leave around until BigQuery table support is removed.
    # consents = fields.Nested(ConsentSchema, many=True)

    biobank_orders = fields.Nested(BiobankOrderSchema, many=True)

    consent_cohort = fields.EnumString(enum=ParticipantCohort)
    consent_cohort_id = fields.EnumInteger(enum=ParticipantCohort)

    patient_statuses = fields.Nested(PatientStatusSchema)

    # PDR-178:  Add CABoR authored to participant top-level schema
    cabor_authored = fields.DateTime()

    deceased_status = fields.EnumString(enum=DeceasedStatus)
    deceased_status_id = fields.EnumInteger(enum=DeceasedStatus)
    deceased_authored = fields.DateTime()

    cohort_2_pilot_flag = fields.EnumString(enum=ParticipantCohortPilotFlag)
    cohort_2_pilot_flag_id = fields.EnumInteger(enum=ParticipantCohortPilotFlag)
    # PDR-166:  Additional EHR status / history information enabled by DA-1781
    is_ehr_data_available = fields.Boolean()
    was_ehr_data_available = fields.Boolean()
    first_ehr_receipt_time = fields.DateTime()
    latest_ehr_receipt_time = fields.DateTime()
    ehr_receipts = fields.Nested(EhrReceiptSchema, many=True)

    # Previously defined Boolean fields converted to UInt8 to support 0/1/2 values
    ubr_sex = fields.UInt8()
    ubr_sexual_orientation = fields.UInt8()
    ubr_gender_identity = fields.UInt8()
    ubr_ethnicity = fields.UInt8()
    ubr_geography = fields.UInt8()
    ubr_education = fields.UInt8()
    ubr_income = fields.UInt8()
    ubr_sexual_gender_minority = fields.UInt8()
    ubr_age_at_consent = fields.UInt8()
    ubr_disability = fields.UInt8()
    ubr_overall = fields.UInt8()

    class Meta:
        schema_id = SchemaID.participant
        resource_uri = 'Participant'
        resource_pk_field = 'participant_id'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ('phone_number', 'login_phone_number', 'email',
                      'distinct_visits', 'first_name', 'middle_name', 'last_name',
                      'sexual_orientation', 'sexual_orientation_id', 'last_modified'
                      ) # List fields that contain PII data

        pii_filter = {}  # dict(field: lambda function).
