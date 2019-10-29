# all BQ tables should be listed here
BQ_TABLES = [
  # (python path, class)
  ('model.bq_participant_summary', 'BQParticipantSummary'),
  ('model.bq_hpo', 'BQHPO'),
  ('model.bq_organization', 'BQOrganization'),
  ('model.bq_site', 'BQSite'),
  ('model.bq_code', 'BQCode'),

  # PDR Tables
  ('model.bq_questionnaires', 'BQPDRConsentPII'),
  ('model.bq_questionnaires', 'BQPDRTheBasics'),
  ('model.bq_questionnaires', 'BQPDRLifestyle'),
  ('model.bq_questionnaires', 'BQPDROverallHealth'),
  ('model.bq_questionnaires', 'BQPDREHRConsentPII'),
  ('model.bq_questionnaires', 'BQPDRDVEHRSharing'),
  ('model.bq_questionnaires', 'BQPDRFamilyHistory'),
  ('model.bq_questionnaires', 'BQPDRHealthcareAccess'),
  ('model.bq_questionnaires', 'BQPDRPersonalMedicalHistory'),

  ('model.bq_pdr_participant_summary', 'BQPDRParticipantSummary'),
]

BQ_VIEWS = [
  # (python path, var)
  ('model.bq_participant_summary', 'BQParticipantSummaryView'),
  ('model.bq_hpo', 'BQHPOView'),
  ('model.bq_organization', 'BQOrganizationView'),
  ('model.bq_site', 'BQSiteView'),
  ('model.bq_code', 'BQCodeView'),
  # PDR Views
  ('model.bq_pdr_participant_summary', 'BQPDRParticipantSummaryView'),
  ('model.bq_pdr_participant_summary', 'BQPDRParticipantSummaryWithdrawnView'),
  ('model.bq_pdr_participant_summary', 'BQPDRPMView'),
  ('model.bq_pdr_participant_summary', 'BQPDRGenderView'),
  ('model.bq_pdr_participant_summary', 'BQPDRRaceView'),
  ('model.bq_pdr_participant_summary', 'BQPDRModuleView'),
  ('model.bq_pdr_participant_summary', 'BQPDRConsentView'),
  ('model.bq_pdr_participant_summary', 'BQPDRBioSpecView'),
  ('model.bq_questionnaires', 'BQPDRConsentPIIView'),
  ('model.bq_questionnaires', 'BQPDRTheBasicsView'),
  ('model.bq_questionnaires', 'BQPDRLifestyleView'),
  ('model.bq_questionnaires', 'BQPDROverallHealthView'),
  ('model.bq_questionnaires', 'BQPDREHRConsentPIIView'),
  ('model.bq_questionnaires', 'BQPDRDVEHRSharingView'),
  ('model.bq_questionnaires', 'BQPDRFamilyHistoryView'),
  ('model.bq_questionnaires', 'BQPDRHealthcareAccessView'),
  ('model.bq_questionnaires', 'BQPDRPersonalMedicalHistoryView'),
]
