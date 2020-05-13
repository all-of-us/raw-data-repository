from enum import Enum

from marshmallow import Schema, validate

from rdr_service.model.resource import fields


class CodeTypeEnum(Enum):
    Module = 1
    Topic = 2
    Question = 3
    Answer = 4


class CodeSchema(Schema):

    id = fields.Int64(required=True)
    created = fields.DateTime(required=True)
    modified = fields.DateTime(required=True)

    code_id = fields.Int32(required=True)
    system = fields.String(validate=validate.Length(max=255), required=True)
    value = fields.String(validate=validate.Length(max=80), required=True)
    display = fields.Text()
    topic = fields.Text()

    code_type = fields.EnumString(enum=CodeTypeEnum, required=True)
    code_type_id = fields.EnumInteger(enum=CodeTypeEnum, required=True)

    mapped = fields.Boolean()
    code_book_id = fields.Int32()
    parent_id = fields.Int32()
    short_value = fields.String(validate=validate.Length(max=50))
