from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum
from rdr_service.model.bq_site import BQObsoleteStatusEnum


class BQOrganizationSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    # Foreign key to awardee/hpo this organization belongs to.
    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    # External ID for the organization, e.g. WISC_MADISON
    external_id = BQField('external_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    # Human readable display name for the organization, e.g. University of Wisconsin, Madison
    display_name = BQField('display_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    is_obsolete = BQField('is_obsolete', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                          fld_enum=BQObsoleteStatusEnum)
    is_obsolete_id = BQField('is_obsolete_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                          fld_enum=BQObsoleteStatusEnum)


class BQOrganization(BQTable):
    """ Organization BigQuery Table """
    __tablename__ = 'organization'
    __schema__ = BQOrganizationSchema


class BQOrganizationView(BQView):
    __viewname__ = 'v_organization'
    __viewdescr__ = 'Organization View'
    __pk_id__ = 'organization_id'
    __table__ = BQOrganization
