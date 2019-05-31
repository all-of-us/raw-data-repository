from enum import Enum

from model.bq_base import BQTable, BQSchema, BQField, BQFieldTypeEnum, BQFieldModeEnum, BQRecordField

class BQStreetAddressTypeEnum(Enum):
  HOME = 1
  MAILING = 2


class BQAddressRecord(BQRecordField):
  """
  Represents a street address.
  Note: Do not use camelCase for property names. Property names must exactly match BQ.
  """
  address_type = BQField('address_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED, BQStreetAddressTypeEnum)
  street_address = BQField('street_address', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  street_address2 = BQField('street_address2', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  city = BQField('city', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  state = BQField('state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  zip = BQField('zip', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQParticipantSummary(BQTable):
  """
  Note: Do not use camelCase for property names. Property names must exactly match BQ.
  """

  __tablename__ = 'participant_summary'

  class __schema__(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    biobank_id = BQField('biobank_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    first_name= BQField('first_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    middle_name = BQField('middle_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    last_name = BQField('last_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    addresses = BQAddressRecord('addresses')
    
