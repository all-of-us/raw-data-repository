from enum import Enum

from model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum
from model.bq_site import BQObsoleteStatusEnum


# TODO: Revert to using participant_enums.py when they have been updated to Python 3.7 Enum classes.
class BQOrganizationTypeEnum(Enum):
  """ A type of organization responsible for signing up participants. """
  UNSET = 0
  HPO = 1  # Healthcare Provider Organization
  FQHC = 2  # Federally Qualified Health Center
  DV = 3  # Direct Volunteer Recruitment Center
  VA = 4  # Veterans Administration


class BQHPOSchema(BQSchema):

  id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
  created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
  modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

  hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
  name = BQField('name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  display_name = BQField('display_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  organization_type = BQField('organization_type', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                              fld_enum=BQOrganizationTypeEnum)
  is_obsolete = BQField('is_obsolete', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                        fld_enum=BQObsoleteStatusEnum)


class BQHPO(BQTable):
  """ HPO BigQuery Table """
  __tablename__ = 'hpo'
  __schema__ = BQHPOSchema
  __project_map__ = [
    ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
  ]


class BQHPOView(BQView):
  __viewname__ = 'v_hpo'
  __viewdescr__ = 'HPO View'
  __table__ = BQHPO