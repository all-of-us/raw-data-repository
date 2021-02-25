from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum


class BQCodeSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    code_id = BQField('code_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    system = BQField('system', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    value = BQField('value', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    display = BQField('display', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    topic = BQField('topic', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    code_type = BQField('code_type', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    mapped = BQField('mapped', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    code_book_id = BQField('code_book_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    parent_id = BQField('parent_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    short_value = BQField('short_value', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    # bq_field_name is generally the same as the value field.  If len(value) > 128, it will generally match
    # short_value.  If value (and  short_value) does not conform to "legal" BigQuery table field name
    # requirements, bq_field_name may be a slightly modified version of value/short_value (e.g., spaces or /
    # replaced with _) that can be used in a BigQuery schema instead
    bq_field_name = BQField('bq_field_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQCode(BQTable):
    """ Code BigQuery Table """
    __tablename__ = 'code'
    __schema__ = BQCodeSchema


class BQCodeView(BQView):
    __viewname__ = 'v_code'
    __viewdescr__ = 'CodeBook Code View'
    __pk_id__ = 'code_id'
    __table__ = BQCode
