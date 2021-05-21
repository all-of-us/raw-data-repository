#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from enum import Enum

from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID
from rdr_service.resource.schemas.site import ObsoleteStatusEnum


class OrganizationTypeEnum(Enum):
    """ A type of organization responsible for signing up participants. """
    UNSET = 0
    HPO = 1  # Healthcare Provider Organization
    FQHC = 2  # Federally Qualified Health Center
    DV = 3  # Direct Volunteer Recruitment Center
    VA = 4  # Veterans Administration


class HPOSchema(Schema):
    hpo_id = fields.Int32(required=True)
    name = fields.String(validate=validate.Length(max=20))
    display_name = fields.String(validate=validate.Length(max=255))
    organization_type = fields.EnumString(enum=OrganizationTypeEnum)
    organization_type_id = fields.EnumInteger(enum=OrganizationTypeEnum)
    is_obsolete = fields.EnumString(enum=ObsoleteStatusEnum)
    is_obsolete_id = fields.EnumInteger(enum=ObsoleteStatusEnum)

    class Meta:
        schema_id = SchemaID.hpo
        resource_uri = 'HPO'
        resource_pk_field = 'hpo_id'
        pii_fields = ('comment', )
