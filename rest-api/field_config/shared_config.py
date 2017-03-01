import concepts
import extraction
import participant_enums

from extraction import UNSET
from questionnaire_response import extractor_for, extract_concept_presence, submission_statuses
from offline.metrics_fields import FieldDef

KNOWN_QUESTIONNAIRES = {
  'consentForStudyEnrollment': concepts.ENROLLMENT_CONSENT_FORM,
  'consentForElectronicHealthRecords': concepts.ELECTRONIC_HEALTH_RECORDS_CONSENT_FORM,
  'questionnaireOnOverallHealth': concepts.OVERALL_HEALTH_PPI_MODULE,
  'questionnaireOnPersonalHabits': concepts.PERSONAL_HABITS_PPI_MODULE,
  'questionnaireOnSociodemographics': concepts.SOCIODEMOGRAPHICS_PPI_MODULE,
  'questionnaireOnHealthcareAccess': concepts.HEALTHCARE_ACCESS_PPI_MODULE,
  'questionnaireOnMedicalHistory': concepts.MEDICAL_HISTORY_PPI_MODULE,
  'questionnaireOnMedications': concepts.MEDICATIONS_PPI_MODULE,
  'questionnaireOnFamilyHealth': concepts.FAMILY_HEALTH_PPI_MODULE
}

# Deprecated. We will be using question_concept_to_field and questionnaire_concept_to_field in
# future.
questionnaire_fields = [
  FieldDef('race',
              extractor_for(concepts.RACE, extraction.VALUE_CODING),
              set(participant_enums.Race)),
  FieldDef('ethnicity',
              extractor_for(concepts.ETHNICITY, extraction.VALUE_CODING),
              set(participant_enums.Ethnicity)),
  FieldDef('membershipTier',
              extractor_for(concepts.MEMBERSHIP_TIER, extraction.VALUE_CODING),
              set(participant_enums.MembershipTier)),
  FieldDef('genderIdentity',
              extractor_for(concepts.GENDER_IDENTITY, extraction.VALUE_CODING),
              set(participant_enums.GenderIdentity)),
  ]  + [FieldDef(k,
    extract_concept_presence(concept),
    set([UNSET]) | submission_statuses())
  for k, concept in KNOWN_QUESTIONNAIRES.iteritems()]

questionnaire_defaults = {f.name: UNSET for f in questionnaire_fields}
