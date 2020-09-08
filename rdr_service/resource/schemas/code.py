#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.model.code import CodeType
from rdr_service.resource import Schema, SchemaMeta, fields
from rdr_service.resource.constants import SchemaID


class CodeSchema(Schema):

    code_id = fields.Int32(required=True)
    system = fields.String(validate=validate.Length(max=255), required=True)
    value = fields.String(validate=validate.Length(max=80), required=True)
    display = fields.Text()
    topic = fields.Text()

    code_type = fields.EnumString(enum=CodeType, required=True)
    code_type_id = fields.EnumInteger(enum=CodeType, required=True)

    mapped = fields.Boolean()
    code_book_id = fields.Int32()
    parent_id = fields.Int32()
    short_value = fields.String(validate=validate.Length(max=50))

    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'code_id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.codes.value,
            type_name=SchemaID.codes.name,
            resource_uri='Codes',
            resource_pk_field='code_id'
        )
