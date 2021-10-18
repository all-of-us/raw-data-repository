from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType

class ConsentMetricSchema(Schema):
    """
    A Consent Metrics data record, based on calculations involving an RDR consent_file record and related
    participant_summary record
    """
    id = fields.Int32(required=True, description='ID field from consent_file table')
    created = fields.DateTime(description='Timestamp for when the consent_file record was created')
    modified = fields.DateTime(description='Timestamp for when the consent_file record was last modified')
    participant_id = fields.String(validate=validate.Length(max=10), required=True)
    participant_origin = fields.String(validate=validate.Length(max=80),
                                       description='Origin/creator of participant (e.g., "vibrent" or "careevolution")')
    hpo = fields.String(validate=validate.Length(max=20), description='HPO participant is paired to')
    hpo_id = fields.Int32()
    organization = fields.String(validate=validate.Length(max=255), description='Organization participant is paired to')
    organization_id = fields.Int32()
    consent_authored_date = fields.DateTime(description='Authored date of this consent')
    sync_status = fields.EnumString(enum=ConsentSyncStatus, required=True,
                                    description='Validation/sync status from the consent_file table')
    sync_status_id = fields.EnumInteger(enum=ConsentSyncStatus, required=True)
    consent_type = fields.EnumString(enum=ConsentType, required=True,
                                     description='Consent type (PRIMARY, CABOR, EHR, GROR, etc.')
    consent_type_id = fields.EnumInteger(enum=ConsentType, required=True)
    resolved_date = fields.Date(description='Last modified timestamp from consent_file, for OBSOLETE sync_status files')
    missing_file = fields.Boolean(description='True if consent PDF file is missing')
    signature_missing = fields.Boolean(description='True if consent PDF file has no signature')
    invalid_signing_date = fields.Boolean(description='True if signing date is outside of valid date range')
    invalid_dob = fields.Boolean(description='True if participant DOB is missing or outside of valid date range')
    invalid_age_at_consent = fields.Boolean(
        description='True if participant age at time of primary consent is under 18, if consent_type is PRIMARY'
    )
    checkbox_unchecked = fields.Boolean(description='True if consent PDF has no checkbox checked')
    non_va_consent_for_va = fields.Boolean(
        description='True if consent for participant paired to VA is not a VA consent form')
    va_consent_for_non_va = fields.Boolean(
        description='True if consent for participant not paired to VA is a VA consent form'
    )
    test_participant = fields.Boolean(description='True if participant id is flagged as a test or ghost participant')
    ignore = fields.Boolean(
        description='True if record should be filtered out of metrics reporting'
    )

    class Meta:
        schema_id = SchemaID.consent_metrics
        resource_uri = 'ConsentMetric'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).
