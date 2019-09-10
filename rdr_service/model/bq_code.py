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


class BQCode(BQTable):
    """ Code BigQuery Table """
    __tablename__ = 'code'
    __schema__ = BQCodeSchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQCodeView(BQView):
    __viewname__ = 'v_code'
    __viewdescr__ = 'CodeBook Code View'
    __table__ = BQCode
