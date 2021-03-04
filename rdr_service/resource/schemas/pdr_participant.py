#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.participant_enums import ParticipantCohort, PhysicalMeasurementsStatus, \
    EnrollmentStatusV2, EhrStatus, WithdrawalStatus, WithdrawalReason, \
    SuspensionStatus, DeceasedStatus, ParticipantCohortPilotFlag
from rdr_service.resource import Schema, fields
from rdr_service.resource.schemas.participant import RaceSchema, GenderSchema, ModuleStatusSchema, ConsentSchema, \
    PatientStatusSchema, BiobankOrderSchema, EHRReceiptSchema
from rdr_service.resource.constants import SchemaID


class PDRPhysicalMeasurementsSchema(Schema):
    """
    PDR Participant Physical Measurements
    """
    status = fields.EnumString(enum=PhysicalMeasurementsStatus)
    status_id = fields.EnumInteger(enum=PhysicalMeasurementsStatus)
    finalized = fields.DateTime()

    class Meta:
        schema_id = SchemaID.participant_physical_measurements
        resource_uri = 'Participant/{participant_id}/PhysicalMeasurements'

class PDRBiobankOrderSchema(BiobankOrderSchema):
    """
    Add additional summary fields to biobank order schema.
    """
    isolate_dna = fields.Boolean()
    isolate_dna_confirmed = fields.Boolean()
    baseline_tests = fields.Int32()
    baseline_tests_confirmed = fields.Int32()


class PDRParticipantSchema(Schema):
    """
    A subset of participant summary for PDR that does not contain PII.
    Note: !!! If you add fields here, remember to add them to the View as well. !!!
    Note: Do not use camelCase for property names. Property names must exactly match BQ
          field names.
    """
    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()

    participant_id = fields.String(validate=validate.Length(max=10), required=True)
    biobank_id = fields.Int32()
    participant_origin = fields.String(validate=validate.Length(max=60))

    addr_state = fields.String(validate=validate.Length(max=2))
    addr_zip = fields.String(validate=validate.Length(max=10))

    is_ghost_id = fields.Boolean()
    test_participant = fields.Boolean()

    sign_up_time = fields.DateTime()
    enrollment_status = fields.EnumString(enum=EnrollmentStatusV2)
    enrollment_status_id = fields.EnumInteger(enum=EnrollmentStatusV2)
    enrollment_member = fields.DateTime()
    enrollment_core_ordered = fields.DateTime()
    enrollment_core_stored = fields.DateTime()
    enrollment_core_minus_pm = fields.DateTime()

    consent_cohort = fields.EnumString(enum=ParticipantCohort)
    consent_cohort_id = fields.EnumInteger(enum=ParticipantCohort)
    cohort_2_pilot_flag = fields.EnumString(enum=ParticipantCohortPilotFlag)
    cohort_2_pilot_flag_id = fields.EnumInteger(enum=ParticipantCohortPilotFlag)

    # PDR-178:  CABoR details.  This is part of ConsentPII, but for various reasons the easiest way to align with
    # RDR CABoR tracking is to surface the appropriate authored date here.  Presence of a date (vs. null/None also
    # acts as the true/false flag equivalent to RDR participant_summary.consent_for_cabor field
    cabor_authored = fields.DateTime()

    email_available = fields.Boolean()
    phone_number_available = fields.Boolean()

    # PDR-106: The EHR fields are needed in PDR by PTSC and for consistency should come from RDR vs. Curation BigQuery
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

    date_of_birth = fields.Date()
    primary_language = fields.String(validate=validate.Length(max=80))

    education = fields.String(validate=validate.Length(max=80))
    education_id = fields.Int32()
    income = fields.String(validate=validate.Length(max=80))
    income_id = fields.Int32()

    sex = fields.String(validate=validate.Length(max=80))
    sex_id = fields.Int32()

    pm = fields.Nested(PDRPhysicalMeasurementsSchema, many=True)

    races = fields.Nested(RaceSchema, many=True)
    genders = fields.Nested(GenderSchema, many=True)
    modules = fields.Nested(ModuleStatusSchema, many=True)
    consents = fields.Nested(ConsentSchema, many=True)

    ubr_sex = fields.Boolean()
    ubr_sexual_orientation = fields.Boolean()
    ubr_gender_identity = fields.Boolean()
    ubr_ethnicity = fields.Boolean()
    ubr_geography = fields.Boolean()
    ubr_education = fields.Boolean()
    ubr_income = fields.Boolean()
    ubr_sexual_gender_minority = fields.Boolean()
    ubr_age_at_consent = fields.Boolean()
    ubr_disability = fields.Boolean()
    ubr_overall = fields.Boolean()

    patient_statuses = fields.Nested(PatientStatusSchema, many=True)

    biobank_orders = fields.Nested(PDRBiobankOrderSchema, many=True)

    # PDR-166:  Additional EHR status / history information enabled by DA-1781
    is_ehr_data_available = fields.Boolean()
    was_ehr_data_available = fields.Boolean()
    first_ehr_receipt_time = fields.DateTime()
    latest_ehr_receipt_time = fields.DateTime()
    ehr_receipts = fields.Nested(EHRReceiptSchema, many=True)

    # PDR-176: Participant deceased status info
    deceased_status = fields.EnumString(enum=DeceasedStatus)
    deceased_status_id = fields.EnumInteger(enum=DeceasedStatus)
    deceased_authored = fields.DateTime()
    # TODO:  Exclude date of death initially in case it constitutes PII, determine if it is needed in PDR
    # date_of_death = fields.Date()

    class Meta:
        schema_id = SchemaID.pdr_participant
        resource_uri = 'PDRParticipant'
        resource_pk_field = 'participant_id'
