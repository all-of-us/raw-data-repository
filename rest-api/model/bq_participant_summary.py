from enum import Enum

from model.bq_base import BQTable, BQSchema, BQField, BQFieldTypeEnum, BQFieldModeEnum, BQRecordField


class BQStreetAddressTypeEnum(Enum):
  RESIDENCE = 1
  MAILING = 2
  EMPLOYMENT = 3


class BQModuleStatusEnum(Enum):
  """The status of a given questionnaire for this participant"""
  UNSET = 0
  SUBMITTED = 1
  SUBMITTED_NO_CONSENT = 2
  SUBMITTED_NOT_SURE = 3
  SUBMITTED_INVALID = 4


class BQAddressSchema(BQSchema):
  """
  Represents a street address schema.
  Note: Do not use camelCase for property names. Property names must exactly match BQ
        field names.
  """
  address_type = BQField('address_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED,
                        fld_enum=BQStreetAddressTypeEnum)
  address_type_id = BQField('address_type_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED,
                            fld_enum=BQStreetAddressTypeEnum)
  street_address_1 = BQField('street_address_1', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  street_address_2 = BQField('street_address_2', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  city = BQField('city', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  state = BQField('state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  country = BQField('country', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  zip = BQField('zip', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQModuleStatusSchema(BQSchema):
  """
  Store information about modules submitted
  """
  module = BQField('module', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  baseline_module = BQField('baseline_module', BQFieldTypeEnum.BOOLEAN, BQFieldModeEnum.NULLABLE)
  authored = BQField('authored', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  created = BQField('created', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  language = BQField('language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  status = BQField('status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE, fld_enum=BQModuleStatusEnum)
  status_id = BQField('status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE, fld_enum=BQModuleStatusEnum)


class BQConsentSchema(BQSchema):
  """
  Store participant consent information
  """
  consent = BQField('consent', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  consent_id = BQField('consent_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  consent_date = BQField('consent_date', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
  consent_value = BQField('consent_value', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  consent_value_id = BQField('consent_value_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQRaceSchema(BQSchema):
  """
  Participant race information
  """
  race = BQField('race', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  race_id = BQField('race_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQGenderSchema(BQSchema):
  """
  Participant gender information
  """
  gender = BQField('gender', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  gender_id = BQField('gender_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPhysicalMeasurements(BQSchema):
  """
  Participant Physical Measurements
  """
  status = BQField('status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  status_id = BQField('status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  created_site = BQField('created_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  created_site_id = BQField('created_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  finalized_site = BQField('finalized_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  finalized_site_id = BQField('finalized_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  finalized = BQField('finalized', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

class BQBiobankSampleSchema(BQSchema):
  """
  Biobank sample information
  """
  test = BQField('test', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  baseline_test = BQField('baseline_test', BQFieldTypeEnum.BOOLEAN, BQFieldModeEnum.NULLABLE)
  dna_test = BQField('dna_test', BQFieldTypeEnum.BOOLEAN, BQFieldModeEnum.NULLABLE)
  collected = BQField('collected', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  processed = BQField('processed', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  finalized = BQField('finalized', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  bb_created = BQField('bb_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  bb_confirmed = BQField('bb_confirmed', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  bb_status = BQField('bb_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  bb_status_id = BQField('bb_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  bb_disposed = BQField('bb_disposed', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  bb_disposed_reason = BQField('bb_disposed_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  bb_disposed_reason_id = BQField('bb_disposed_reason_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQBiobankOrderSchema(BQSchema):
  """
  Biobank order information
  """
  biobank_order_id = BQField('biobank_order_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  status = BQField('status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  status_id = BQField('status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  dv_order = BQField('dv_order', BQFieldTypeEnum.BOOLEAN, BQFieldModeEnum.NULLABLE)
  collected_site = BQField('collected_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  collected_site_id = BQField('collected_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  processed_site = BQField('processed_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  processed_site_id = BQField('processed_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  finalized_site = BQField('finalized_site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  finalized_site_id = BQField('finalized_site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  samples = BQRecordField('samples', schema=BQBiobankSampleSchema)


class BQParticipantSummarySchema(BQSchema):
  """
  Note: Do not use camelCase for property names. Property names must exactly match BQ
        field names.
  """
  id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
  created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
  modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

  last_modified = BQField('last_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

  participant_id = BQField('participant_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
  biobank_id = BQField('biobank_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

  first_name = BQField('first_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  middle_name = BQField('middle_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  last_name = BQField('last_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

  is_ghost_id = BQField('is_ghost_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

  sign_up_time = BQField('sign_up_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  enrollment_status = BQField('enrollment_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  enrollment_status_id = BQField('enrollment_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

  withdrawal_status = BQField('withdrawal_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  withdrawal_status_id = BQField('withdrawal_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  withdrawal_time = BQField('withdrawal_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
  withdrawal_reason = BQField('withdrawal_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  withdrawal_reason_id = BQField('withdrawal_reason_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  withdrawal_reason_justification = BQField('withdrawal_reason_justification', BQFieldTypeEnum.STRING,
                                            BQFieldModeEnum.NULLABLE)
  suspension_status = BQField('suspension_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  suspension_status_id = BQField('suspension_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  suspension_time = BQField('suspension_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

  hpo = BQField('hpo', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
  hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
  organization = BQField('organization', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  site = BQField('site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

  distinct_visits = BQField('distinct_visits', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

  date_of_birth = BQField('date_of_birth', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
  language = BQField('language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  language_id = BQField('language_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  primary_language = BQField('primary_language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

  contact_method = BQField('contact_method', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  contact_method_id = BQField('contact_method_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  phone_number = BQField('phone_number', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  login_phone_number = BQField('login_phone_number', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  email = BQField('email', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

  addresses = BQRecordField('addresses', schema=BQAddressSchema)

  education = BQField('education', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  education_id = BQField('education_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
  income = BQField('income', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  income_id = BQField('income_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

  sexual_orientation = BQField('sexual_orientation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
  sexual_orientation_id = BQField('sexual_orientation_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

  pm = BQRecordField('pm', schema=BQPhysicalMeasurements)

  races = BQRecordField('races', schema=BQRaceSchema)
  genders = BQRecordField('genders', schema=BQGenderSchema)
  modules = BQRecordField('modules', schema=BQModuleStatusSchema)
  consents = BQRecordField('consents', schema=BQConsentSchema)

  biobank_orders = BQRecordField('biobank_orders', schema=BQBiobankOrderSchema)


class BQParticipantSummary(BQTable):
  """ Participant Summary BigQuery Table """
  __tablename__ = 'participant_summary'
  __schema__ = BQParticipantSummarySchema
