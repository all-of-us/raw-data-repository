#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID, RetentionStatusEnum, RetentionTypeEnum


class RetentionMetricSchema(Schema):
    id = fields.Int32(required=True)
    created = fields.DateTime()
    modified = fields.DateTime()
    participant_id = fields.String(validate=validate.Length(max=10), required=True)
    retention_eligible = fields.Boolean()
    retention_eligible_time = fields.DateTime()
    actively_retained = fields.Boolean()
    passively_retained = fields.Boolean()
    file_upload_date = fields.DateTime()
    retention_eligible_status = fields.EnumString(enum=RetentionStatusEnum)
    retention_eligible_status_id = fields.EnumInteger(enum=RetentionStatusEnum)
    retention_type = fields.EnumString(enum=RetentionTypeEnum)
    retention_type_id = fields.EnumInteger(enum=RetentionTypeEnum)

    class Meta:
        schema_id = SchemaID.retention_metrics
        resource_uri = 'RetentionMetrics'
        resource_pk_field = 'participant_id'
        pii_fields = None
