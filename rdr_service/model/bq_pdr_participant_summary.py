#
# BigQuery schemas for PDR that do not contain PII.
#
#
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField
from rdr_service.model.bq_participant_summary import (
    BQRaceSchema,
    BQGenderSchema,
    BQModuleStatusSchema,
    BQConsentSchema, BQPatientStatusSchema
)


class BQPDRPhysicalMeasurements(BQSchema):
    """
    PDR Participant Physical Measurements
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
    participant_origin = BQField('participant_origin', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    addr_state = BQField('addr_state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    addr_zip = BQField('addr_zip', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    is_ghost_id = BQField('is_ghost_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

    sign_up_time = BQField('sign_up_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_status = BQField('enrollment_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enrollment_status_id = BQField('enrollment_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    enrollment_member = BQField('enrollment_member', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_core_ordered = BQField('enrollment_core_ordered', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    enrollment_core_stored = BQField('enrollment_core_stored', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    # These EHR fields are being replaced by a direct query from Curation's BigQuery database and need to be removed.
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
    ubr_age_at_consent = BQField('ubr_age_at_consent', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    consent_cohort = BQField('consent_cohort', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    consent_cohort_id = BQField('consent_cohort_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    email_available = BQField('email_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    phone_number_available = BQField('phone_number_available', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    ubr_disability = BQField('ubr_disability', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    patient_statuses = BQRecordField('patient_statuses', schema=BQPatientStatusSchema)

    test_participant = BQField('test_participant', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQPDRParticipantSummary(BQTable):
    """ PDR Participant Summary BigQuery Table """
    __tablename__ = 'pdr_participant'
    __schema__ = BQPDRParticipantSummarySchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDRParticipantSummaryView(BQView):
    """ PDR Team view of the Participant Summary """
    __viewname__ = 'v_pdr_participant'
    __viewdescr__ = 'PDR Participant Summary View'
    __table__ = BQPDRParticipantSummary
    __pk_id__ = 'participant_id'
    # We need to build a SQL statement with all fields except sub-tables and remove duplicates.
    __sql__ = """
        SELECT
          %%FIELD_NAMES%%
        FROM (
            SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
                MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
              FROM `{project}`.{dataset}.pdr_participant
          ) ps
          WHERE ps.modified = ps.max_timestamp and ps.withdrawal_status_id = 1 and ps.max_test_participant != 1
      """.replace('%%FIELD_NAMES%%', BQPDRParticipantSummarySchema.get_sql_field_names(
        exclude_fields=[
            'pm',
            'genders',
            'races',
            'modules',
            'consents',
            'biospec',
            'patent_statuses'
        ])
    )


class BQPDRParticipantSummaryWithdrawnView(BQView):
    __viewname__ = 'v_pdr_participant_withdrawn'
    __viewdescr__ = 'PDR Participant Summary Withdrawn View'
    __table__ = BQPDRParticipantSummary
    __sql__ = BQPDRParticipantSummaryView.__sql__.replace('ps.withdrawal_status_id = 1',
                                                          'ps.withdrawal_status_id != 1')


class BQPDRPMView(BQView):
    __viewname__ = 'v_pdr_participant_pm'
    __viewdescr__ = 'PDR Physical Measurements View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(pm) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """


class BQPDRGenderView(BQView):
    __viewname__ = 'v_pdr_participant_gender'
    __viewdescr__ = 'PDR Participant Gender View'
    __table__ = BQPDRParticipantSummary
    __pk_id__ = 'participant_id'
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(genders) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """


class BQPDRRaceView(BQView):
    __viewname__ = 'v_pdr_participant_race'
    __viewdescr__ = 'PDR Participant Race View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(races) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """


class BQPDRModuleView(BQView):
    __viewname__ = 'v_pdr_participant_module'
    __viewdescr__ = 'PDR Participant Survey Module View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id,
           nt.mod_module, nt.mod_baseline_module,
           CAST(nt.mod_authored AS DATETIME) as mod_authored, CAST(nt.mod_created AS DATETIME) as mod_created,
           nt.mod_language, nt.mod_status, nt.mod_status_id
      FROM (
        SELECT *,
            MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(modules) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """


class BQPDRConsentView(BQView):
    __viewname__ = 'v_pdr_participant_consent'
    __viewdescr__ = 'PDR Participant Consent View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(consents) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """


class BQPDRBioSpecView(BQView):
    __viewname__ = 'v_pdr_biospec'
    __viewdescr__ = 'PDR Participant BioBank Order View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(biospec) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """

class BQPDRPatientStatuesView(BQView):
    __viewname__ = 'v_pdr_participant_patient_status'
    __viewdescr__ = 'PDR Participant Patient Status View'
    __table__ = BQPDRParticipantSummary
    __sql__ = """
    SELECT ps.id, ps.created, ps.modified, ps.participant_id, nt.*
      FROM (
        SELECT *, MAX(modified) OVER (PARTITION BY participant_id) AS max_timestamp,
            MAX(test_participant) OVER (PARTITION BY participant_id) AS max_test_participant
          FROM `{project}`.{dataset}.pdr_participant
      ) ps cross join unnest(patient_statuses) as nt
      WHERE ps.modified = ps.max_timestamp and ps.max_test_participant != 1
  """
