import clock

from participant_enums import HPOId, PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import MembershipTier, GenderIdentity, Ethnicity, Race, get_bucketed_age
from model.base import Base
from model.utils import to_upper, Enum
from sqlalchemy import Column, Integer, String, Date, DateTime, BLOB
from sqlalchemy import UniqueConstraint, ForeignKey, func, Index, SmallInteger
from sqlalchemy.orm import relationship

class ParticipantSummary(Base):  
  __tablename__ = 'participant_summary'
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'), 
                         primary_key=True, autoincrement=False)
  biobankId = Column('biobank_id', Integer, nullable=False)
  firstName = Column('first_name', String(80))
  firstNameUpper = Column('first_name_upper', String(80), onupdate=to_upper('first_name'), 
                          default=to_upper('first_name'))
  middleName = Column('middle_name', String(80))
  middleNameUpper = Column('middle_name_upper', String(80), onupdate=to_upper('middle_name'),
                           default=to_upper('middle_name'))
  lastName = Column('last_name', String(80))
  lastNameUpper = Column('last_name_upper', String(80), onupdate=to_upper('last_name'),
                         default=to_upper('last_name'))
  zipCode = Column('zip_code', String(10))
  dateOfBirth = Column('date_of_birth', Date)
  genderIdentity = Column('gender_identity', Enum(GenderIdentity), default=GenderIdentity.UNSET)
  membershipTier = Column('membership_tier', Enum(MembershipTier), default=MembershipTier.UNSET)
  race = Column('race', Enum(Race), default=Race.UNSET)
  ethnicity = Column('ethnicity', Enum(Ethnicity), default=Ethnicity.UNSET)
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
Index('participant_summary_ln_dob', ParticipantSummary.lastNameUpper, 
      ParticipantSummary.dateOfBirth)
Index('participant_summary_ln_dob_zip', ParticipantSummary.lastNameUpper, 
      ParticipantSummary.dateOfBirth, ParticipantSummary.zipCode)
Index('participant_summary_ln_dob_fn', ParticipantSummary.lastNameUpper, 
      ParticipantSummary.dateOfBirth, ParticipantSummary.firstNameUpper)
Index('participant_summary_hpo', ParticipantSummary.hpoId)
Index('participant_summary_hpo_fn', ParticipantSummary.hpoId, ParticipantSummary.firstNameUpper)
Index('participant_summary_hpo_ln', ParticipantSummary.hpoId, ParticipantSummary.lastNameUpper)
Index('participant_summary_hpo_dob', ParticipantSummary.hpoId, ParticipantSummary.dateOfBirth)
Index('participant_summary_hpo_ethnicity', ParticipantSummary.hpoId, ParticipantSummary.ethnicity)
Index('participant_summary_hpo_zip', ParticipantSummary.hpoId, ParticipantSummary.zipCode)
Index('participant_summary_hpo_tier', ParticipantSummary.hpoId, ParticipantSummary.membershipTier)
Index('participant_summary_hpo_consent', ParticipantSummary.hpoId, 
      ParticipantSummary.consentForStudyEnrollment)
Index('participant_summary_hpo_num_baseline_ppi', ParticipantSummary.hpoId, 
      ParticipantSummary.numCompletedBaselinePPIModules)
Index('participant_summary_hpo_num_baseline_samples', ParticipantSummary.hpoId, 
      ParticipantSummary.numBaselineSamplesArrived)
