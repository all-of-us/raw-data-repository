# all BQ tables should be listed here
BQ_TABLES = [
    # (python path, class)
    ('rdr_service.model.bq_participant_summary', 'BQParticipantSummary'),
    ('rdr_service.model.bq_hpo', 'BQHPO'),
    ('rdr_service.model.bq_organization', 'BQOrganization'),
    ('rdr_service.model.bq_site', 'BQSite'),
    ('rdr_service.model.bq_code', 'BQCode'),

    # PDR Tables
    ('rdr_service.model.bq_questionnaires', 'BQPDRConsentPII'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRTheBasics'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRLifestyle'),
    ('rdr_service.model.bq_questionnaires', 'BQPDROverallHealth'),
    ('rdr_service.model.bq_questionnaires', 'BQPDREHRConsentPII'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRDVEHRSharing'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRFamilyHistory'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRHealthcareAccess'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRPersonalMedicalHistory'),

    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRParticipantSummary'),
]

BQ_VIEWS = [
    # (python path, var)
    ('rdr_service.model.bq_participant_summary', 'BQParticipantSummaryView'),
    ('rdr_service.model.bq_hpo', 'BQHPOView'),
    ('rdr_service.model.bq_organization', 'BQOrganizationView'),
    ('rdr_service.model.bq_site', 'BQSiteView'),
    ('rdr_service.model.bq_code', 'BQCodeView'),
    # PDR Views
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRParticipantSummaryView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRParticipantSummaryWithdrawnView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRPMView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRGenderView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRRaceView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRModuleView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRConsentView'),
    ('rdr_service.model.bq_pdr_participant_summary', 'BQPDRBioSpecView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRConsentPIIView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRTheBasicsView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRLifestyleView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDROverallHealthView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDREHRConsentPIIView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRDVEHRSharingView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRFamilyHistoryView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRHealthcareAccessView'),
    ('rdr_service.model.bq_questionnaires', 'BQPDRPersonalMedicalHistoryView'),
]
