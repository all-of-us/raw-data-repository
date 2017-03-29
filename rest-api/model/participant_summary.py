import clock

from api_util import format_json_date, format_json_enum, format_json_code, format_json_hpo
from code_constants import UNSET
from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import EnrollmentStatus, Race, get_bucketed_age, SampleStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
from model.base import Base
from model.utils import Enum, to_client_participant_id, to_client_biobank_id
from sqlalchemy import Column, Integer, String, Date, DateTime
from sqlalchemy import ForeignKey, Index, SmallInteger
from sqlalchemy.orm import relationship


_DATE_FIELDS = ['dateOfBirth', 'signUpTime', 'consentForStudyEnrollmentTime',
                'consentForElectronicHealthRecordsTime', 'questionnaireOnOverallHealthTime',
                'questionnaireOnLifestyleTime', 'questionnaireOnTheBasicsTime',
                'questionnaireOnHealthcareAccessTime', 'questionnaireOnMedicalHistoryTime',
                'questionnaireOnMedicationsTime', 'questionnaireOnFamilyHealthTime']
_ENUM_FIELDS = ['enrollmentStatus', 'race', 'physicalMeasurementsStatus',
                'consentForStudyEnrollment', 'consentForElectronicHealthRecords',
                'questionnaireOnOverallHealth', 'questionnaireOnLifestyle',
                'questionnaireOnTheBasics', 'questionnaireOnHealthcareAccess',
                'questionnaireOnMedicalHistory', 'questionnaireOnMedications',
                'questionnaireOnFamilyHealth', 'suspensionStatus', 'withdrawalStatus',
                'samplesToIsolateDNA']
_CODE_FIELDS = ['genderIdentityId']


class ParticipantSummary(Base):
  __tablename__ = 'participant_summary'
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         primary_key=True, autoincrement=False)
  biobankId = Column('biobank_id', Integer, nullable=False)
  firstName = Column('first_name', String(80), nullable=False)
  middleName = Column('middle_name', String(80))
  lastName = Column('last_name', String(80), nullable=False)
  zipCode = Column('zip_code', String(10))
  state = Column('state', String(2))
  dateOfBirth = Column('date_of_birth', Date)
  genderIdentityId = Column('gender_identity_id', Integer, ForeignKey('code.code_id'))
  enrollmentStatus = Column('enrollment_status', Enum(EnrollmentStatus),
                            default=EnrollmentStatus.INTERESTED)
  race = Column('race', Enum(Race), default=Race.UNSET)
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
  questionnaireOnLifestyle = Column('questionnaire_on_lifestyle',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnLifestyleTime = Column('questionnaire_on_lifestyle_time', DateTime)
  questionnaireOnTheBasics = Column('questionnaire_on_the_basics',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnTheBasicsTime = Column('questionnaire_on_the_basics_time', DateTime)
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
  # The number of BiobankStoredSamples recorded for this participant, limited to those samples
  # where testCode is one of the baseline tests (listed in the config).
  numBaselineSamplesArrived = Column('num_baseline_samples_arrived', SmallInteger, default=0)
  samplesToIsolateDNA = Column('samples_to_isolate_dna', Enum(SampleStatus),
                               default=SampleStatus.UNSET)

  # Withdrawal from the study of the participant's own accord.
  withdrawalStatus = Column(
      'withdrawal_status',
      Enum(WithdrawalStatus),
      nullable=False,
      onupdate=WithdrawalStatus.NOT_WITHDRAWN)

  suspensionStatus = Column(
      'suspension_status',
      Enum(SuspensionStatus),
      nullable=False,
      onupdate=SuspensionStatus.NOT_SUSPENDED)

  participant = relationship("Participant", back_populates="participantSummary")

  def to_client_json(self):
    result = self.asdict()
    result['participantId'] = to_client_participant_id(self.participantId)
    result['biobankId'] = to_client_biobank_id(self.biobankId)
    date_of_birth = result.get('dateOfBirth')
    if date_of_birth:
      result['ageRange'] = get_bucketed_age(date_of_birth, clock.CLOCK.now())
    else:
      result['ageRange'] = UNSET
    format_json_hpo(result, 'hpoId')
    for fieldname in _DATE_FIELDS:
      format_json_date(result, fieldname)
    for fieldname in _CODE_FIELDS:
      format_json_code(result, fieldname)
    for fieldname in _ENUM_FIELDS:
      format_json_enum(result, fieldname)
    # Strip None values.
    result = {k: v for k, v in result.iteritems() if v is not None}

    return result

# TODO(DA-234) Add an index for withdrawal status when querying summaries & filtering by withdrawal.
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
Index('participant_summary_hpo_race', ParticipantSummary.hpoId, ParticipantSummary.race)
Index('participant_summary_hpo_zip', ParticipantSummary.hpoId, ParticipantSummary.zipCode)
Index('participant_summary_hpo_status', ParticipantSummary.hpoId,
      ParticipantSummary.enrollmentStatus)
Index('participant_summary_hpo_consent', ParticipantSummary.hpoId,
      ParticipantSummary.consentForStudyEnrollment)
Index('participant_summary_hpo_num_baseline_ppi', ParticipantSummary.hpoId,
      ParticipantSummary.numCompletedBaselinePPIModules)
Index('participant_summary_hpo_num_baseline_samples', ParticipantSummary.hpoId,
      ParticipantSummary.numBaselineSamplesArrived)
