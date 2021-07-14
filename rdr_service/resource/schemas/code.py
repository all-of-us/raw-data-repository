#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.model.code import CodeType
from rdr_service.resource import Schema, fields
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
        schema_id = SchemaID.codes
        resource_uri = 'Codes'
        resource_pk_field = 'code_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).
