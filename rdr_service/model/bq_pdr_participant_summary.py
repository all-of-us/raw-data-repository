#
# BigQuery schemas for PRD that do not contain PII.
#
#
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField
from rdr_service.model.bq_participant_summary import (
    BQRaceSchema,
    BQGenderSchema,
    BQModuleStatusSchema,
    BQConsentSchema
)


class BQPDRPhysicalMeasurements(BQSchema):
    """
    PRD Participant Physical Measurements
    """
    pm_status = BQField('pm_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pm_status_id = BQField('pm_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    pm_finalized = BQField('pm_finalized', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQPDRBiospecimenSchema(BQSchema):
    """
    PDR Summary of Biobank Orders and Tests
    """
    biosp_status = BQField('biosp_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    biosp_status_id = BQField('biosp_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biosp_order_time = BQField('biosp_order_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    biosp_isolate_dna = BQField('biosp_isolate_dna', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPDRParticipantSummarySchema(BQSchema):
    """
    A subset of participant summary for PDR that does not contain PII.
    Note: !!! If you add fields here, remember to add them to the View as well. !!!
    Note: Do not use camelCase for property names. Property names must exactly match BQ
          field names.
    """
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    participant_id = BQField('participant_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    addr_city = BQField('addr_city', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_state = BQField('addr_state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_zip = BQField('addr_zip', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    is_ghost_id = BQField('is_ghost_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

    sign_up_time = BQField('sign_up_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status = BQField('enrollment_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enrollment_status_id = BQField('enrollment_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    enrollment_member = BQField('enrollment_member', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_core_ordered = BQField('enrollment_core_ordered', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_core_stored = BQField('enrollment_core_stored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    ehr_status = BQField('ehr_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    ehr_status_id = BQField('ehr_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ehr_receipt = BQField('ehr_receipt', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    ehr_update = BQField('ehr_update', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    withdrawal_status = BQField('withdrawal_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    withdrawal_status_id = BQField('withdrawal_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    withdrawal_time = BQField('withdrawal_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    withdrawal_authored = BQField('withdrawal_authored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    withdrawal_reason = BQField('withdrawal_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    withdrawal_reason_id = BQField('withdrawal_reason_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    withdrawal_reason_justification = BQField('withdrawal_reason_justification', BQFieldTypeEnum.STRING,
                                              BQFieldModeEnum.NULLABLE)

    hpo = BQField('hpo', BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    organization = BQField('organization', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    site = BQField('site', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    date_of_birth = BQField('date_of_birth', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
    primary_language = BQField('primary_language', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    education = BQField('education', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    education_id = BQField('education_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    income = BQField('income', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    income_id = BQField('income_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    sex = BQField('sex', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_id = BQField('sex_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    pm = BQRecordField('pm', schema=BQPDRPhysicalMeasurements)

    races = BQRecordField('races', schema=BQRaceSchema)
    genders = BQRecordField('genders', schema=BQGenderSchema)
    modules = BQRecordField('modules', schema=BQModuleStatusSchema)
    consents = BQRecordField('consents', schema=BQConsentSchema)

    biospec = BQRecordField('biospec', schema=BQPDRBiospecimenSchema)

    ubr_sex = BQField('ubr_sex', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_sexual_orientation = BQField('ubr_sexual_orientation', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_gender_identity = BQField('ubr_gender_identity', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_ethnicity = BQField('ubr_ethnicity', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_geography = BQField('ubr_geography', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_education = BQField('ubr_education', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_income = BQField('ubr_income', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ubr_sexual_gender_minority = BQField('ubr_sexual_gender_minority', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    ubr_overall = BQField('ubr_overall', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPDRParticipantSummary(BQTable):
    """ PRD Participant Summary BigQuery Table """
    __tablename__ = 'pdr_participant'
    __schema__ = BQPDRParticipantSummarySchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDRParticipantSummaryView(BQView):
    """ PRD Team view of the Participant Summary """
    __viewname__ = 'v_pdr_participant'
    __viewdescr__ = 'PRD Participant Summary View'
    __table__ = BQPDRParticipantSummary
    # Manually define the fields, because we need to break out the sub-tables.
    __sql__ = """
    SELECT 
      ps.participant_id,
      ps.addr_city,
      ps.addr_state,
      ps.addr_zip,
      ps.is_ghost_id,
      ps.sign_up_time,
      ps.enrollment_status,
      ps.enrollment_status_id,
      ps.enrollment_member,
      ps.enrollment_core_ordered,
      ps.enrollment_core_stored,
      ps.ehr_status,
      ps.ehr_status_id,
      ps.ehr_receipt,
      ps.ehr_update,
      ps.withdrawal_status,
      ps.withdrawal_status_id,
      ps.withdrawal_time,
      ps.withdrawal_authored,
      ps.withdrawal_reason,
      ps.withdrawal_reason_id,
      ps.withdrawal_reason_justification,
      ps.hpo,
      ps.hpo_id,
      ps.organization,
      ps.organization_id,
      ps.site,
      ps.site_id,
      ps.date_of_birth,
      ps.primary_language,
      ps.education,
      ps.education_id,
      ps.income,
      ps.income_id,
      ps.sex,
      ps.sex_id,
      ps.ubr_sex,
      ps.ubr_sexual_orientation,
      ps.ubr_gender_identity,
      ps.ubr_ethnicity,
      ps.ubr_geography,
      ps.ubr_education,
      ps.ubr_income,
      ps.ubr_sexual_gender_minority,
      ps.ubr_overall    
    FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY id) AS max_timestamp
          FROM `{project}`.{dataset}.pdr_participant 
      ) ps
      WHERE ps.modified = ps.max_timestamp and ps.withdrawal_status_id = 1
  """


class BQPDRParticipantSummaryWithdrawnView(BQView):
    __viewname__ = 'v_pdr_participant_withdrawn'
    __viewdescr__ = 'PRD Participant Summary Withdrawn View'
    __table__ = BQPDRParticipantSummary
    __sql__ = BQPDRParticipantSummaryView.__sql__.replace('ps.withdrawal_status_id = 1',
                                                          'ps.withdrawal_status_id != 1')


class BQPDRPMView(BQView):
    __viewname__ = 'v_pdr_participant_pm'
    __viewdescr__ = 'PRD Physical Measurements View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY id) AS max_timestamp
          FROM `{project}`.{dataset}.pdr_participant 
      ) ps cross join unnest(pm) as nt
      WHERE ps.modified = ps.max_timestamp
  """


class BQPDRGenderView(BQView):
    __viewname__ = 'v_pdr_participant_gender'
    __viewdescr__ = 'PRD Participant Gender View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY id) AS max_timestamp
          FROM `{project}`.{dataset}.pdr_participant 
      ) ps cross join unnest(genders) as nt
      WHERE ps.modified = ps.max_timestamp  
  """


class BQPDRRaceView(BQView):
    __viewname__ = 'v_pdr_participant_race'
    __viewdescr__ = 'PRD Participant Race View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY id) AS max_timestamp
          FROM `{project}`.{dataset}.pdr_participant 
      ) ps cross join unnest(races) as nt
      WHERE ps.modified = ps.max_timestamp
  """


class BQPDRModuleView(BQView):
    __viewname__ = 'v_pdr_participant_module'
    __viewdescr__ = 'PRD Participant Survey Module View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY id) AS max_timestamp
          FROM `{project}`.{dataset}.pdr_participant 
      ) ps cross join unnest(modules) as nt
      WHERE ps.modified = ps.max_timestamp 
  """


class BQPDRConsentView(BQView):
    __viewname__ = 'v_pdr_participant_consent'
    __viewdescr__ = 'PRD Participant Consent View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY id) AS max_timestamp
          FROM `{project}`.{dataset}.pdr_participant 
      ) ps cross join unnest(consents) as nt
      WHERE ps.modified = ps.max_timestamp 
  """
