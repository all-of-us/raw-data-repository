#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import date
from enum import Enum
from marshmallow import validate

from rdr_service.participant_enums import QuestionnaireStatus, ParticipantCohort, Race, GenderIdentity, \
    PhysicalMeasurementsStatus, OrderStatus, EnrollmentStatusV2, EhrStatus, WithdrawalStatus, WithdrawalReason, \
    SuspensionStatus
from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID


class StreetAddressTypeEnum(Enum):
    RESIDENCE = 1
    MAILING = 2
    EMPLOYMENT = 3


COHORT_1_CUTOFF = date(2018, 4, 24)
COHORT_2_CUTOFF = date(2020, 4, 16)


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
        resource_pk_field = 'addr_type_id'


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

    class Meta:
        schema_id = SchemaID.participant_modules
        resource_uri = 'Participant/{participant_id}/Modules'
        resource_pk_field = 'module'


class ConsentSchema(Schema):
    """ Store participant consent information """
    consent = fields.String(validate=validate.Length(max=80))
    consent_id = fields.Int32()
    consent_date = fields.Date()
    consent_value = fields.String(validate=validate.Length(max=80))
    consent_value_id = fields.Int32()
    consent_module = fields.String(validate=validate.Length(max=80))
    consent_module_authored = fields.DateTime()
    consent_module_created = fields.DateTime()
    consent_expired = fields.String(validate=validate.Length(max=80))
    consent_module_external_id = fields.String(validate=validate.Length(max=120))

    class Meta:
        schema_id = SchemaID.participant_consents
        resource_uri = 'Participant/{participant_id}/Consents'
        resource_pk_field = 'consent_id'


class RaceSchema(Schema):
    """ Participant race information """
    race = fields.EnumString(enum=Race)
    race_id = fields.EnumInteger(enum=Race)

    class Meta:
        schema_id = SchemaID.participant_race
        resource_uri = 'Participant/{participant_id}/Races'
        resource_pk_field = 'race_id'


class GenderSchema(Schema):
    """ Participant gender information """
    gender = fields.EnumString(GenderIdentity)
    gender_id = fields.EnumInteger(GenderIdentity)

    class Meta:
        schema_id = SchemaID.participant_gender
        resource_uri = 'Participant/{participant_id}/Genders'
        resource_pk_field = 'gender_id'


class PhysicalMeasurementsSchema(Schema):
    """ Participant Physical Measurements """
    physical_measurements_id = fields.Int32()
    status = fields.EnumString(enum=PhysicalMeasurementsStatus)
    status_id = fields.EnumInteger(enum=PhysicalMeasurementsStatus)
    created = fields.DateTime()
    created_site = fields.String(validate=validate.Length(max=255))
    created_site_id = fields.Int32()
    finalized_site = fields.String(validate=validate.Length(max=255))
    finalized_site_id = fields.Int32()
    finalized = fields.DateTime()

    class Meta:
        schema_id = SchemaID.participant_physical_measurements
        resource_uri = 'Participant/{participant_id}/PhysicalMeasurements'
        resource_pk_field = 'physical_measurements_id'


class BiobankSampleSchema(Schema):
    """ Biobank sample information """
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

    class Meta:
        schema_id = SchemaID.participant_biobank_order_samples
        resource_uri = 'Participant/{participant_id}/BiobankOrders/{biobank_order_id}/Samples'
        resource_pk_field = 'test'


class BiobankOrderSchema(Schema):
    """
    Biobank order information
    """
    biobank_order_id = fields.String(validate=validate.Length(max=80))
    order_created = fields.DateTime()
    status = fields.EnumString(enum=OrderStatus)
    status_id = fields.EnumInteger(enum=OrderStatus)
    dv_order = fields.Boolean()
    collected_site = fields.String(validate=validate.Length(max=255))
    collected_site_id = fields.Int32()
    processed_site = fields.String(validate=validate.Length(max=255))
    processed_site_id = fields.Int32()
    finalized_site = fields.String(validate=validate.Length(max=255))
    finalized_site_id = fields.Int32()
    samples = fields.Nested(BiobankSampleSchema, many=True)
    tests_ordered = fields.Int32()
    tests_stored = fields.Int32()

    class Meta:
        schema_id = SchemaID.participant_biobank_orders
        resource_uri = 'Participant/{participant_id}/BiobankOrders'
        resource_pk_field = 'biobank_order_id'


class PatientStatusSchema(Schema):
    """
    Patient Status History: PatientStatusFlag Enum
    """
    patent_status_history_id = fields.Int32()
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
        resource_pk_field = 'patent_status_history_id'


class ParticipantSchema(Schema):
    """ Participant Activity Summary Schema """
    last_modified = fields.DateTime()

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
    enrollment_status = fields.EnumString(enum=EnrollmentStatusV2)
    enrollment_status_id = fields.EnumInteger(enum=EnrollmentStatusV2)
    enrollment_member = fields.DateTime()
    enrollment_core_ordered = fields.DateTime()
    enrollment_core_stored = fields.DateTime()

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

    addresses = fields.Nested(AddressSchema, many=True)

    education = fields.String(validate=validate.Length(max=80))
    education_id = fields.Int32()
    income = fields.String(validate=validate.Length(max=80))
    income_id = fields.Int32()

    sex = fields.String(validate=validate.Length(max=80))
    sex_id = fields.Int32()
    sexual_orientation = fields.String(validate=validate.Length(max=80))
    sexual_orientation_id = fields.Int32()

    pm = fields.Nested(PhysicalMeasurementsSchema, many=True)

    races = fields.Nested(RaceSchema, many=True)
    genders = fields.Nested(GenderSchema, many=True)
    modules = fields.Nested(ModuleStatusSchema, many=True)
    consents = fields.Nested(ConsentSchema, many=True)

    biobank_orders = fields.Nested(BiobankOrderSchema, many=True)

    consent_cohort = fields.EnumString(enum=ParticipantCohort)
    consent_cohort_id = fields.EnumInteger(enum=ParticipantCohort)

    patient_statuses = fields.Nested(PatientStatusSchema)

    # PDR-178:  Add CABoR authored to participant top-level schema
    cabor_authored = fields.DateTime()

    class Meta:
        schema_id = SchemaID.participant
        resource_uri = 'Participant'
        resource_pk_field = 'participant_id'
