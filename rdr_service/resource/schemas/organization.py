#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.resource import Schema, SchemaMeta, fields
from rdr_service.resource.schemas import SchemaUniqueIds


class OrganizationSchema(Schema):
    """ HPO Organization Schema """
    # Foreign key to awardee/hpo this organization belongs to.
    hpo_id = fields.Int32()
    organization_id = fields.Int32()
    # External ID for the organization, e.g. WISC_MADISON
    external_id = fields.String(validate=validate.Length(max=80))
    # Human readable display name for the organization, e.g. University of Wisconsin, Madison
    display_name = fields.String(validate=validate.Length(max=255))
    is_obsolete = fields.Boolean()

    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'organization_id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaUniqueIds.organization.value,
            type_name=SchemaUniqueIds.organization.name,
            resource_uri='Organization',
            resource_pk_field='organization_id'
        )