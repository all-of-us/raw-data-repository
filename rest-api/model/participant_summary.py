from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import MembershipTier
from model.base import Base
from model.utils import Enum
from sqlalchemy import Column, Integer, String, Date, DateTime
from sqlalchemy import ForeignKey, Index, SmallInteger
from sqlalchemy.orm import relationship

class ParticipantSummary(Base):  
  __tablename__ = 'participant_summary'
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'), 
                         primary_key=True, autoincrement=False)
  biobankId = Column('biobank_id', Integer, nullable=False)
  firstName = Column('first_name', String(80))
  middleName = Column('middle_name', String(80))
  lastName = Column('last_name', String(80))
  zipCode = Column('zip_code', String(10))
  dateOfBirth = Column('date_of_birth', Date)
  genderIdentityId = Column('gender_identity_id', Integer, ForeignKey('code.code_id'))
  # Does membershipTier come from questionnaires? Should this be an FK?
  membershipTier = Column('membership_tier', Enum(MembershipTier), default=MembershipTier.UNSET)
  raceId = Column('race_id', Integer, ForeignKey('code.code_id'))
  ethnicityId = Column('ethnicity_id', Integer, ForeignKey('code.code_id'))
  physicalMeasurementsStatus = Column('physical_measurements_status', 
                                      Enum(PhysicalMeasurementsStatus), 
                                      default=PhysicalMeasurementsStatus.UNSET)
  signUpTime = Column('sign_up_time', DateTime)
  hpoId = Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)
  consentForStudyEnrollment = Column('consent_for_study_enrollment', 
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  consentForStudyEnrollmentTime = Column('consent_for_study_enrollment_time', DateTime)
  consentForElectronicHealthRecords = Column('consent_for_electronic_health_records',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  consentForElectronicHealthRecordsTime = Column('consent_for_electronic_health_records_time', 
                                                 DateTime)
  questionnaireOnOverallHealth = Column('questionnaire_on_overall_health',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnOverallHealthTime = Column('questionnaire_on_overall_health_time', DateTime)
  questionnaireOnPersonalHabits = Column('questionnaire_on_personal_habits',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnPersonalHabitsTime = Column('questionnaire_on_personal_habits_time', DateTime)      
  questionnaireOnSociodemographics = Column('questionnaire_on_sociodemographics',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnSociodemographicsTime = Column('questionnaire_on_sociodemographics_time', DateTime)
  questionnaireOnHealthcareAccess = Column('questionnaire_on_healthcare_access',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnHealthcareAccessTime = Column('questionnaire_on_healthcare_access_time', DateTime)
  questionnaireOnMedicalHistory = Column('questionnaire_on_medical_history',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnMedicalHistoryTime = Column('questionnaire_on_medical_history_time', DateTime)
  questionnaireOnMedications = Column('questionnaire_on_medications',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnMedicationsTime = Column('questionnaire_on_medications_time', DateTime)
  questionnaireOnFamilyHealth = Column('questionnaire_on_family_health',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnFamilyHealthTime = Column('questionnaire_on_family_health_time', DateTime)
  numCompletedBaselinePPIModules = Column('num_completed_baseline_ppi_modules', SmallInteger, 
                                          default=0)
  numBaselineSamplesArrived = Column('num_baseline_samples_arrived', SmallInteger, default=0)
  
  participant = relationship("Participant", back_populates="participantSummary")
      
Index('participant_summary_biobank_id', ParticipantSummary.biobankId)
Index('participant_summary_ln_dob', ParticipantSummary.lastName, 
      ParticipantSummary.dateOfBirth)
Index('participant_summary_ln_dob_zip', ParticipantSummary.lastName, 
      ParticipantSummary.dateOfBirth, ParticipantSummary.zipCode)
Index('participant_summary_ln_dob_fn', ParticipantSummary.lastName, 
      ParticipantSummary.dateOfBirth, ParticipantSummary.firstName)
Index('participant_summary_hpo', ParticipantSummary.hpoId)
Index('participant_summary_hpo_fn', ParticipantSummary.hpoId, ParticipantSummary.firstName)
Index('participant_summary_hpo_ln', ParticipantSummary.hpoId, ParticipantSummary.lastName)
Index('participant_summary_hpo_dob', ParticipantSummary.hpoId, ParticipantSummary.dateOfBirth)
Index('participant_summary_hpo_ethnicity', ParticipantSummary.hpoId,
      ParticipantSummary.ethnicityId)
Index('participant_summary_hpo_zip', ParticipantSummary.hpoId, ParticipantSummary.zipCode)
Index('participant_summary_hpo_tier', ParticipantSummary.hpoId, ParticipantSummary.membershipTier)
Index('participant_summary_hpo_consent', ParticipantSummary.hpoId,
      ParticipantSummary.consentForStudyEnrollment)
Index('participant_summary_hpo_num_baseline_ppi', ParticipantSummary.hpoId, 
      ParticipantSummary.numCompletedBaselinePPIModules)
Index('participant_summary_hpo_num_baseline_samples', ParticipantSummary.hpoId, 
      ParticipantSummary.numBaselineSamplesArrived)
