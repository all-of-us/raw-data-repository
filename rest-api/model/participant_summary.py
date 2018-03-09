import datetime
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy import ForeignKey, Index, SmallInteger
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from model.base import Base
from model.utils import Enum
from model.utils import UTCDateTime
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrderStatus
from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus
from participant_enums import WithdrawalStatus, SuspensionStatus
# The only fields that can be returned, queried on, or ordered by for queries for withdrawn
# participants.
WITHDRAWN_PARTICIPANT_FIELDS = ['withdrawalStatus', 'withdrawalTime', 'participantId', 'hpoId',
                                'biobankId', 'firstName', 'middleName', 'lastName', 'dateOfBirth',
                                'consentForStudyEnrollment', 'consentForStudyEnrollmentTime',
                                'consentForElectronicHealthRecords',
                                'consentForElectronicHealthRecordsTime']
# The period of time for which withdrawn participants will still be returned in results for
# queries that don't ask for withdrawn participants.
WITHDRAWN_PARTICIPANT_VISIBILITY_TIME = datetime.timedelta(days=2)

class ParticipantSummary(Base):
  """Summary fields extracted from participant data (combined from multiple tables).
  Consented participants only."""
  __tablename__ = 'participant_summary'
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         primary_key=True, autoincrement=False)
  biobankId = Column('biobank_id', Integer, nullable=False)
  lastModified = Column('last_modified', UTCDateTime, nullable=False)
  # PTC string fields will generally be limited to 255 chars; set our field lengths accordingly to
  # ensure that long values can be inserted.
  firstName = Column('first_name', String(255), nullable=False)
  middleName = Column('middle_name', String(255))
  lastName = Column('last_name', String(255), nullable=False)
  zipCode = Column('zip_code', String(10))
  stateId = Column('state_id', Integer, ForeignKey('code.code_id'))
  city = Column('city', String(255))
  streetAddress = Column('street_address', String(255))
  phoneNumber = Column('phone_number', String(80))
  email = Column('email', String(255), nullable=False)
  recontactMethodId = Column('recontact_method_id', Integer, ForeignKey('code.code_id'))
  languageId = Column('language_id', Integer, ForeignKey('code.code_id'))
  dateOfBirth = Column('date_of_birth', Date)
  genderIdentityId = Column('gender_identity_id', Integer, ForeignKey('code.code_id'))
  sexId = Column('sex_id', Integer, ForeignKey('code.code_id'))
  sexualOrientationId = Column('sexual_orientation_id', Integer, ForeignKey('code.code_id'))
  educationId = Column('education_id', Integer, ForeignKey('code.code_id'))
  incomeId = Column('income_id', Integer, ForeignKey('code.code_id'))
  enrollmentStatus = Column('enrollment_status', Enum(EnrollmentStatus),
                            default=EnrollmentStatus.INTERESTED)
  race = Column('race', Enum(Race), default=Race.UNSET)
  physicalMeasurementsStatus = Column('physical_measurements_status',
                                      Enum(PhysicalMeasurementsStatus),
                                      default=PhysicalMeasurementsStatus.UNSET)
  # The first time that physical measurements were submitted for the participant.
  physicalMeasurementsTime = Column('physical_measurements_time', UTCDateTime)
  # The time that physical measurements were finalized (before submission to the RDR)
  physicalMeasurementsFinalizedTime = Column('physical_measurements_finalized_time', UTCDateTime)
  physicalMeasurementsCreatedSiteId = Column('physical_measurements_created_site_id', Integer,
                                             ForeignKey('site.site_id'))
  physicalMeasurementsFinalizedSiteId = Column('physical_measurements_finalized_site_id', Integer,
                                               ForeignKey('site.site_id'))

  signUpTime = Column('sign_up_time', UTCDateTime)

  # Fields for which questionnaires have been submitted, and at what times.
  consentForStudyEnrollment = Column('consent_for_study_enrollment',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  consentForStudyEnrollmentTime = Column('consent_for_study_enrollment_time', UTCDateTime)
  consentForElectronicHealthRecords = Column('consent_for_electronic_health_records',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  consentForElectronicHealthRecordsTime = Column('consent_for_electronic_health_records_time',
                                                 UTCDateTime)
  consentForCABoR = Column('consent_for_cabor',
                           Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  consentForCABoRTime = Column('consent_for_cabor_time', UTCDateTime)
  questionnaireOnOverallHealth = Column('questionnaire_on_overall_health',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnOverallHealthTime = Column('questionnaire_on_overall_health_time', UTCDateTime)
  questionnaireOnLifestyle = Column('questionnaire_on_lifestyle',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnLifestyleTime = Column('questionnaire_on_lifestyle_time', UTCDateTime)
  questionnaireOnTheBasics = Column('questionnaire_on_the_basics',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnTheBasicsTime = Column('questionnaire_on_the_basics_time', UTCDateTime)
  questionnaireOnHealthcareAccess = Column('questionnaire_on_healthcare_access',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnHealthcareAccessTime = Column('questionnaire_on_healthcare_access_time',
                                               UTCDateTime)
  questionnaireOnMedicalHistory = Column('questionnaire_on_medical_history',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnMedicalHistoryTime = Column('questionnaire_on_medical_history_time', UTCDateTime)
  questionnaireOnMedications = Column('questionnaire_on_medications',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnMedicationsTime = Column('questionnaire_on_medications_time', UTCDateTime)
  questionnaireOnFamilyHealth = Column('questionnaire_on_family_health',
      Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
  questionnaireOnFamilyHealthTime = Column('questionnaire_on_family_health_time', UTCDateTime)

  # Fields for which samples have been received, and at what times.
  sampleStatus1SST8 = Column('sample_status_1sst8', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1SST8Time = Column('sample_status_1sst8_time', UTCDateTime)
  sampleStatus2SST8 = Column('sample_status_2sst8', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus2SST8Time = Column('sample_status_2sst8_time', UTCDateTime)
  sampleStatus1SS08 = Column('sample_status_1ss08', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1SS08Time = Column('sample_status_1ss08_time', UTCDateTime)
  sampleStatus1PST8 = Column('sample_status_1pst8', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1PST8Time = Column('sample_status_1pst8_time', UTCDateTime)
  sampleStatus2PST8 = Column('sample_status_2pst8', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus2PST8Time = Column('sample_status_2pst8_time', UTCDateTime)
  sampleStatus1PS08 = Column('sample_status_1ps08', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1PS08Time = Column('sample_status_1ps08_time', UTCDateTime)
  sampleStatus1HEP4 = Column('sample_status_1hep4', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1HEP4Time = Column('sample_status_1hep4_time', UTCDateTime)
  sampleStatus1ED04 = Column('sample_status_1ed04', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1ED04Time = Column('sample_status_1ed04_time', UTCDateTime)
  sampleStatus1ED10 = Column('sample_status_1ed10', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1ED10Time = Column('sample_status_1ed10_time', UTCDateTime)
  sampleStatus2ED10 = Column('sample_status_2ed10', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus2ED10Time = Column('sample_status_2ed10_time', UTCDateTime)
  sampleStatus1UR10 = Column('sample_status_1ur10', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1UR10Time = Column('sample_status_1ur10_time', UTCDateTime)
  sampleStatus1SAL = Column('sample_status_1sal', Enum(SampleStatus), default=SampleStatus.UNSET)
  sampleStatus1SALTime = Column('sample_status_1sal_time', UTCDateTime)

  # Fields for which samples have been ordered, and at what times.
  sampleOrderStatus1SST8 = Column('sample_order_status_1sst8', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1SST8Time = Column('sample_order_status_1sst8_time', UTCDateTime)
  sampleOrderStatus2SST8 = Column('sample_order_status_2sst8', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus2SST8Time = Column('sample_order_status_2sst8_time', UTCDateTime)
  sampleOrderStatus1SS08 = Column('sample_order_status_1ss08', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1SS08Time = Column('sample_order_status_1ss08_time', UTCDateTime)
  sampleOrderStatus1PST8 = Column('sample_order_status_1pst8', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1PST8Time = Column('sample_order_status_1pst8_time', UTCDateTime)
  sampleOrderStatus2PST8 = Column('sample_order_status_2pst8', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus2PST8Time = Column('sample_order_status_2pst8_time', UTCDateTime)
  sampleOrderStatus1PS08 = Column('sample_order_status_1ps08', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1PS08Time = Column('sample_order_status_1ps08_time', UTCDateTime)
  sampleOrderStatus1HEP4 = Column('sample_order_status_1hep4', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1HEP4Time = Column('sample_order_status_1hep4_time', UTCDateTime)
  sampleOrderStatus1ED04 = Column('sample_order_status_1ed04', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1ED04Time = Column('sample_order_status_1ed04_time', UTCDateTime)
  sampleOrderStatus1ED10 = Column('sample_order_status_1ed10', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1ED10Time = Column('sample_order_status_1ed10_time', UTCDateTime)
  sampleOrderStatus2ED10 = Column('sample_order_status_2ed10', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus2ED10Time = Column('sample_order_status_2ed10_time', UTCDateTime)
  sampleOrderStatus1UR10 = Column('sample_order_status_1ur10', Enum(OrderStatus),
                                  default=OrderStatus.UNSET)
  sampleOrderStatus1UR10Time = Column('sample_order_status_1ur10_time', UTCDateTime)
  sampleOrderStatus1SAL = Column('sample_order_status_1sal', Enum(OrderStatus),
                                 default=OrderStatus.UNSET)
  sampleOrderStatus1SALTime = Column('sample_order_status_1sal_time', UTCDateTime)

  numCompletedBaselinePPIModules = Column('num_completed_baseline_ppi_modules', SmallInteger,
                                          default=0)
  numCompletedPPIModules = Column('num_completed_ppi_modules', SmallInteger, default=0)

  # The number of BiobankStoredSamples recorded for this participant, limited to those samples
  # where testCode is one of the baseline tests (listed in the config).
  numBaselineSamplesArrived = Column('num_baseline_samples_arrived', SmallInteger, default=0)
  samplesToIsolateDNA = Column('samples_to_isolate_dna', Enum(SampleStatus),
                               default=SampleStatus.UNSET)
  # Whether biospecimens have been finalized or not, and the time at which they were
  # finalized.
  biospecimenStatus = Column('biospecimen_status', Enum(OrderStatus), default=OrderStatus.UNSET)
  biospecimenOrderTime = Column('biospecimen_order_time', UTCDateTime)
  biospecimenSourceSiteId = Column('biospecimen_source_site_id', Integer,
                                   ForeignKey('site.site_id'))
  biospecimenCollectedSiteId = Column('biospecimen_collected_site_id', Integer,
                                      ForeignKey('site.site_id'))
  biospecimenProcessedSiteId = Column('biospecimen_processed_site_id', Integer,
                                      ForeignKey('site.site_id'))
  biospecimenFinalizedSiteId = Column('biospecimen_finalized_site_id', Integer,
                                      ForeignKey('site.site_id'))

  # Withdrawal from the study of the participant's own accord.
  withdrawalStatus = Column(
      'withdrawal_status',
      Enum(WithdrawalStatus),
      nullable=False,
      onupdate=WithdrawalStatus.NOT_WITHDRAWN)
  withdrawalTime = Column('withdrawal_time', UTCDateTime)

  suspensionStatus = Column(
      'suspension_status',
      Enum(SuspensionStatus),
      nullable=False,
      onupdate=SuspensionStatus.NOT_SUSPENDED)
  suspensionTime = Column('suspension_time', UTCDateTime)

  participant = relationship("Participant", back_populates="participantSummary")

  @declared_attr
  def hpoId(cls):
    return Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)

  @declared_attr
  def organizationId(cls):
    return Column('organization_id', Integer, ForeignKey('organization.organization_id'))

  @declared_attr
  def siteId(cls):
    return Column('site_id', Integer, ForeignKey('site.site_id'))


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
Index('participant_summary_hpo_withdrawal_status_time', ParticipantSummary.hpoId,
      ParticipantSummary.withdrawalStatus, ParticipantSummary.withdrawalTime)
