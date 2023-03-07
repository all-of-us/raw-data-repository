#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from enum import Enum

from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID


# TODO: RDR-PDR pipeline requires Enum classes to exist in PDR codebase, so define them here for easier migration
# RDR Enum class:  OnsiteVerificationType
class PDROnsiteVerificationType(Enum):
    """Types of on site verification"""
    UNSET = 0
    PHOTO_AND_ONE_OF_PII = 1
    TWO_OF_PII = 2

# RDR Enum class: OnsiteVerificationVisitType
class PDROnsiteVerificationVisitType(Enum):
    """Types of on site visit"""
    UNSET = 0
    PMB_INITIAL_VISIT = 1
    PHYSICAL_MEASUREMENTS_ONLY = 2
    BIOSPECIMEN_COLLECTION_ONLY = 3
    BIOSPECIMEN_REDRAW_ONLY = 4
    RETENTION_ACTIVITIES = 5


class OnSiteIdVerificationSchema(Schema):
    """
    A Participant OnSiteIdVerification record
    """
    id = fields.Int32(required=True)
    created = fields.DateTime()
    modified = fields.DateTime()
    participant_id = fields.String(validate=validate.Length(max=10), required=True)
    site = fields.String(validate=validate.Length(max=255),
                         description='google_group string associated with the site')
    site_id = fields.Int32(required=False)
    verification_type = fields.EnumString(enum=PDROnsiteVerificationType)
    verification_type_id = fields.EnumInteger(enum=PDROnsiteVerificationType)
    visit_type = fields.EnumString(enum=PDROnsiteVerificationVisitType)
    visit_type_id = fields.EnumInteger(enum=PDROnsiteVerificationVisitType)
    verification_time = fields.DateTime()

    class Meta:
        schema_id = SchemaID.onsite_id_verification
        resource_uri = 'OnSiteIdVerification'
        resource_pk_field = 'id'
        pii_fields = ('user_email',)  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function)
