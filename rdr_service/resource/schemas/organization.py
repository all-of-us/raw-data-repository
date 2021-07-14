#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.schemas.site import ObsoleteStatusEnum
from rdr_service.resource.constants import SchemaID


class OrganizationSchema(Schema):
    """ HPO Organization Schema """
    # Foreign key to awardee/hpo this organization belongs to.
    hpo_id = fields.Int32()
    organization_id = fields.Int32()
    # External ID for the organization, e.g. WISC_MADISON
    external_id = fields.String(validate=validate.Length(max=80))
    # Human readable display name for the organization, e.g. University of Wisconsin, Madison
    display_name = fields.String(validate=validate.Length(max=255))
    is_obsolete = fields.EnumString(enum=ObsoleteStatusEnum)
    is_obsolete_id = fields.EnumInteger(enum=ObsoleteStatusEnum)

    class Meta:
        schema_id = SchemaID.organization
        resource_uri = 'Organization'
        resource_pk_field = 'organization_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).
