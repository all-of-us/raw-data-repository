import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    UnicodeText,
    event)
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship
from sqlalchemy.sql import expression

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, EnumZeroBased, UTCDateTime, UTCDateTime6
from rdr_service.participant_enums import (
    EhrStatus,
    EnrollmentStatus,
    GenderIdentity,
    OrderStatus,
    PhysicalMeasurementsStatus,
    QuestionnaireStatus,
    Race,
    SampleCollectionMethod,
    SampleStatus,
    SuspensionStatus,
    WithdrawalReason,
    WithdrawalStatus,
    ParticipantCohort,
    ParticipantCohortPilotFlag,
    ConsentExpireStatus,
    DeceasedStatus,
    RetentionStatus)


# The only fields that can be returned, queried on, or ordered by for queries for withdrawn
# participants.
WITHDRAWN_PARTICIPANT_FIELDS = [
    "withdrawalStatus",
    "withdrawalTime",
    "withdrawalAuthored",
    "withdrawalReason",
    "withdrawalReasonJustification",
    "participantId",
    "hpoId",
    "organizationId",
    "siteId",
    "biobankId",
    "firstName",
    "middleName",
    "lastName",
    "dateOfBirth",
    "consentForStudyEnrollment",
    "consentForStudyEnrollmentAuthored",
    "consentForElectronicHealthRecords",
    "consentForElectronicHealthRecordsAuthored",
]

# The period of time for which withdrawn participants will still be returned in results for
# queries that don't ask for withdrawn participants.
WITHDRAWN_PARTICIPANT_VISIBILITY_TIME = datetime.timedelta(days=2)

RETENTION_WINDOW = datetime.timedelta(days=547)

# suspended or deceased participants don't allow contact but can still use samples. These fields
# will not be returned when queried on suspended participant.
SUSPENDED_OR_DECEASED_PARTICIPANT_FIELDS = ["zipCode", "city", "streetAddress", "streetAddress2", "phoneNumber",
                                            "loginPhoneNumber", "email"]

# SQL Conditional for participant's retention eligibility computed column (1 = NOT_ELIGIBLE, 2 = ELIGIBLE)
_COMPUTE_RETENTION_ELIGIBLE_SQL = """
    CASE WHEN
      consent_for_study_enrollment = 1
      AND (
        consent_for_electronic_health_records_first_yes_authored is not null OR
        consent_for_dv_electronic_health_records_sharing = 1
      )
      AND questionnaire_on_the_basics = 1
      AND questionnaire_on_overall_health = 1
      AND questionnaire_on_lifestyle = 1
      AND samples_to_isolate_dna = 1
      AND withdrawal_status = 1
      AND suspension_status = 1
      AND deceased_status = 0
    THEN 2 ELSE 1
    END
"""

# SQL for calculating the date when a participant gained retention eligibility
# Null unless the participant meets the retention-eligible requirements (above) and a qualifying test sample time
# is present.  Otherwise, find the last of the consent / module authored dates and the earliest of the qualifying
# DNA test samples.  The retention eligibility date is the later of those two
_COMPUTE_RETENTION_ELIGIBLE_TIME_SQL = """
     CASE WHEN retention_eligible_status = 2 AND
          COALESCE(sample_status_1ed10_time, sample_status_2ed10_time, sample_status_1ed04_time,
                 sample_status_1sal_time, sample_status_1sal2_time, 0) != 0
        THEN GREATEST(
            GREATEST(consent_for_study_enrollment_first_yes_authored,
             COALESCE(baseline_questionnaires_first_complete_authored,
                 GREATEST(questionnaire_on_the_basics_authored,
                          questionnaire_on_overall_health_authored,
                          questionnaire_on_lifestyle_authored)
             ),
             COALESCE(consent_for_electronic_health_records_first_yes_authored,
             consent_for_study_enrollment_first_yes_authored),
             COALESCE(consent_for_dv_electronic_health_records_sharing_authored,
             consent_for_study_enrollment_first_yes_authored)
            ),
            LEAST(COALESCE(sample_status_1ed10_time, '9999-01-01'),
                COALESCE(sample_status_2ed10_time, '9999-01-01'),
                COALESCE(sample_status_1ed04_time, '9999-01-01'),
                COALESCE(sample_status_1sal_time, '9999-01-01'),
                COALESCE(sample_status_1sal2_time, '9999-01-01')
            )
        )
        ELSE NULL
     END
"""


class ParticipantSummary(Base):
    """Summary fields extracted from participant data (combined from multiple tables).
    Consented participants only."""

    __tablename__ = "participant_summary"
    participantId = Column(
        "participant_id", Integer, ForeignKey("participant.participant_id"), primary_key=True, autoincrement=False
    )
    'The RDR internal unique ID of a participant.'

    biobankId = Column("biobank_id", Integer, nullable=False)
    """
    PMI-specific ID generated by the RDR and used exclusively for communicating with the biobank.
    Human-readable 10-character string beginning with prefix specific to the environment ("A" for production).
    """

    # PTC string fields will generally be limited to 255 chars; set our field lengths accordingly to
    # ensure that long values can be inserted.
    firstName = Column("first_name", String(255), nullable=False)
    'The first name of the participant.'

    middleName = Column("middle_name", String(255))
    'The middle name of the participant.'

    lastName = Column("last_name", String(255), nullable=False)
    'The last name of the participant.'

    zipCode = Column("zip_code", String(10))
    'The postal zip code of the participant.'

    stateId = Column("state_id", Integer, ForeignKey("code.code_id"))
    state = None  # placeholder for docs, API sets on model using corresponding ID field
    'The state the participant lives in.'

    city = Column("city", String(255))
    'The city the participant lives in.'

    streetAddress = Column("street_address", String(255))
    'Line 1 of the street address the participant lives at.'

    streetAddress2 = Column("street_address2", String(255))
    'Line 2 of the street address the participant lives at. Absent if no line 2 given.'

    phoneNumber = Column("phone_number", String(80))
    'The phone number of the participant.'

    loginPhoneNumber = Column("login_phone_number", String(80))
    'Verified phone number. Participants must provide loginPhoneNumber or email for registration.'

    email = Column("email", String(255))
    'Email address to register a participant. Participants must provide loginPhoneNumber or email for registration.'

    primaryLanguage = Column("primary_language", String(80))
    'Indicates the language of the consent that the participant signed. We only have "en" or "es" for now.'

    recontactMethodId = Column("recontact_method_id", Integer, ForeignKey("code.code_id"))
    recontactMethod = None  # placeholder for docs, API sets on model using corresponding ID field
    'Which method the participant would like used for contact. i.e. phone or email.'


    # deprecated - will remove languageId in the future
    languageId = Column("language_id", Integer, ForeignKey("code.code_id"))
    language = None  # placeholder for docs, API sets on model using corresponding ID field
    '*Deprecated*'


    dateOfBirth = Column("date_of_birth", Date)
    'The day the participant was born.'

    ageRange = None  # placeholder for docs, API sets on model using corresponding ID field
    """
    The "bucketed" age range of participant.

    :ref:`Enumerated values <age_range>`
    """

    genderIdentityId = Column("gender_identity_id", Integer, ForeignKey("code.code_id"))
    genderIdentity = Column("gender_identity", Enum(GenderIdentity))
    """
    The personal sense of one's own gender. It can correlate with assigned sex at birth or can differ from it.

    :ref:`Enumerated values <gender_identity>`
    """

    sexId = Column("sex_id", Integer, ForeignKey("code.code_id"))
    sex = None  # placeholder for docs, API sets on model using corresponding ID field
    'Assigned sex at birth.'

    sexualOrientationId = Column("sexual_orientation_id", Integer, ForeignKey("code.code_id"))
    sexualOrientation = None  # placeholder for docs, API sets on model using corresponding ID field
    """
    A person's sexual identity in relation to the gender to which they are attracted.
    """

    race = Column("race", Enum(Race), default=Race.UNSET)
    """
    A race is a grouping of humans based on shared physical or social qualities into categories generally viewed as
    distinct by society. First used to refer to speakers of a common language and then to denote national affiliations,
    by the 17th century the term race began to refer to physical (phenotypical) traits.

    :ref:`Enumerated values <race>`
    """

    educationId = Column("education_id", Integer, ForeignKey("code.code_id"))
    education = None  # placeholder for docs, API sets on model using corresponding ID field
    'The highest level of education the participant has received.'

    incomeId = Column("income_id", Integer, ForeignKey("code.code_id"))
    income = None  # placeholder for docs, API sets on model using corresponding ID field
    'The participants income. Income is defined as a persons salary in a given year.'

    deceasedStatus = Column(
        "deceased_status",
        EnumZeroBased(DeceasedStatus),
        nullable=False,
        default=DeceasedStatus.UNSET
    )
    """
    Indicates whether the participant has a PENDING or APPROVED deceased reports.

    Will be UNSET for participants that have no deceased reports or only DENIED reports.
    """

    deceasedAuthored = Column("deceased_authored", UTCDateTime)
    """
    The UTC timestamp of when the report was entered into an external system,
    or when it was approved externally if it has been approved.
    """

    dateOfDeath = Column("date_of_death", Date)
    'Date of death if provided on the deceased report or when it was reviewed.'

    hpoId = Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"), nullable=False)
    """
    HPO marked as primary for this participant,
    if any (just the resource id, like PITT — not a reference like Organization/PITT)
    """

    awardee = None  # placeholder for docs, API sets this equal to the hpoId
    "Copy of the hpoId field"

    @declared_attr
    def organizationId(cls):
        return Column("organization_id", Integer, ForeignKey("organization.organization_id"))
    organization = None  # placeholder for docs, API sets on model using corresponding ID field
    "An organization a participant is paired with or UNSET if none."

    @declared_attr
    def siteId(cls):
        return Column("site_id", Integer, ForeignKey("site.site_id"))
    site = None  # placeholder for docs, API sets on model using corresponding ID field
    "A physical location a participant is paired with or UNSET if none."

    @declared_attr
    def enrollmentSiteId(cls):
        return Column("enrollment_site_id", Integer, ForeignKey("site.site_id"))
    enrollmentSite = None  # placeholder for docs, API sets on model using corresponding ID field
    "A physical location a participant is enrolled with or UNSET if none."

    enrollmentStatus = Column("enrollment_status", Enum(EnrollmentStatus), default=EnrollmentStatus.INTERESTED)
    """
    Depends on a number of factors including questionnaires and biobank samples completed

    :ref:`Enumerated values <enrollment_status>`
    """

    # The time that this participant become a member
    enrollmentStatusMemberTime = Column("enrollment_status_member_time", UTCDateTime)
    """
    Present when a participant has completed an EHR consent,
    is the timestamp of when that consent was received by the API
    """

    # The time when we get the first stored sample
    enrollmentStatusCoreStoredSampleTime = Column("enrollment_status_core_stored_sample_time", UTCDateTime)
    """
    Present when a participant has completed all baseline modules, physical measurements, and has DNA samples stored.

    Is the latest date from the list of:

    * The earliest date of sampleStatus...Time (any of the DNA sample tests)
    * enrollmentStatusMemberTime
    * questionnaireOnTheBasicsTime
    * questionnaireOnLifestyleTime
    * questionnaireOnOverallHealthTime
    * physicalMeasurementsFinalizedTime
    """

    # The time when we get a DNA order
    enrollmentStatusCoreOrderedSampleTime = Column("enrollment_status_core_ordered_sample_time", UTCDateTime)
    """
    Present when a participant has completed all baseline modules, physical measurements, and has DNA samples stored.

    Is the latest date from the list of:

    * The earliest date of sampleOrderStatus...Time (any of the DNA sample tests)
    * enrollmentStatusMemberTime
    * questionnaireOnTheBasicsTime
    * questionnaireOnLifestyleTime
    * questionnaireOnOverallHealthTime
    * physicalMeasurementsFinalizedTime
    """

    consentCohort = Column("consent_cohort", Enum(ParticipantCohort), default=ParticipantCohort.UNSET)
    """
    Cohort assignment based on the date the participant enrolled in the program.

    :ref:`Enumerated values <consent_cohort>`
    """

    cohort2PilotFlag = Column(
        "cohort_2_pilot_flag", Enum(ParticipantCohortPilotFlag), default=ParticipantCohortPilotFlag.UNSET
    )
    """
    Indicates whether a participant was designated for the Genomics pilot.
    The pilot participants were only drawn from Cohort 2.

    :ref:`Enumerated values <cohort_2_pilot_flag>`
    """

    # EHR status related columns
    ehrStatus = Column("ehr_status", Enum(EhrStatus), default=EhrStatus.NOT_PRESENT)
    """
    .. warning::
        DEPRECATED - use :py:attr:`wasEhrDataAvailable` instead.

    Indicates whether Electronic Health Records (EHR) have ever been present for the participant.

    :ref:`Enumerated values <ehr_status>`
    """

    ehrReceiptTime = Column("ehr_receipt_time", UTCDateTime)
    """
    .. warning::
        DEPRECATED - use :py:attr:`firstEhrReceiptTime` instead.

    UTC timestamp indicating when RDR was first made aware of signed and uploaded EHR documents
    """

    ehrUpdateTime = Column("ehr_update_time", UTCDateTime)
    """
    .. warning::
        DEPRECATED - use :py:attr:`latestEhrReceiptTime` instead.

    UTC timestamp indicating the latest time RDR was aware of signed and uploaded EHR documents
    """

    isEhrDataAvailable = Column(
        "is_ehr_data_available",
        Boolean,
        nullable=False,
        server_default=expression.false()
    )
    """
    A true or false value that indicates whether Electronic Health Records (EHR)
    are currently present for the participant.
    """

    wasEhrDataAvailable = None  # Placeholder filled in by the DAO using the value in ehrStatus
    """
    A true or false value that indicates whether Electronic Health Records (EHR)
    have ever been present for the participant.
    """

    firstEhrReceiptTime = None  # Placeholder filled in by the DAO using the value in ehrReceiptTime
    """
    UTC timestamp indicating when RDR was first made aware of signed and uploaded EHR documents
    """

    latestEhrReceiptTime = None  # Placeholder filled in by the DAO using the value in ehrUpdateTime
    """
    UTC timestamp indicating the latest time RDR was aware of signed and uploaded EHR documents
    """

    physicalMeasurementsStatus = Column(
        "physical_measurements_status", Enum(PhysicalMeasurementsStatus), default=PhysicalMeasurementsStatus.UNSET
    )
    """
    Indicates whether this participant has completed physical measurements.

    :ref:`Enumerated values <physical_measurements_status>`
    """

    # The first time that physical measurements were submitted for the participant.
    physicalMeasurementsTime = Column("physical_measurements_time", UTCDateTime)
    'Indicates the latest time physical measurements were submitted for the participant'

    # The time that physical measurements were finalized (before submission to the RDR)
    physicalMeasurementsFinalizedTime = Column("physical_measurements_finalized_time", UTCDateTime)
    'Indicates the latest time physical measurements were finalized for the participant'

    physicalMeasurementsCreatedSiteId = Column(
        "physical_measurements_created_site_id", Integer, ForeignKey("site.site_id")
    )
    physicalMeasurementsCreatedSite = None  # placeholder for docs, API sets on model using corresponding ID field
    'Indicates the site where physical measurements were created for the participant'

    physicalMeasurementsFinalizedSiteId = Column(
        "physical_measurements_finalized_site_id", Integer, ForeignKey("site.site_id")
    )
    physicalMeasurementsFinalizedSite = None  # placeholder for docs, API sets on model using corresponding ID field
    'Indicates the site where physical measurements were finalized for the participant'

    numberDistinctVisits = Column("number_distinct_visits", Integer, default=0)
    'The number of distinct visits to a health care provider that the participant has made that supplied data'

    signUpTime = Column("sign_up_time", UTCDateTime)
    'The time at which the participant initially signed up for All Of Us'

    withdrawalStatus = Column("withdrawal_status", Enum(WithdrawalStatus), nullable=False)
    """
    The status of withdrawal for a participant.

    :ref:`Enumerated values <withdrawal_status>`
    """

    withdrawalReason = Column("withdrawal_reason", Enum(WithdrawalReason))
    """
    If withdrawalReason is UNSET the participant is self withdrawn,
    any other enumeration means the participant was administratively withdrawn.

    :ref:`Enumerated values <withdrawal_reason>`
    """

    withdrawalTime = Column("withdrawal_time", UTCDateTime)
    "The time that the API received the participant's withdrawal."

    withdrawalAuthored = Column("withdrawal_authored", UTCDateTime)
    "The time the participant withdrew from program participation."

    withdrawalReasonJustification = Column("withdrawal_reason_justification", UnicodeText)
    'Withdrawal reason free text field'

    suspensionStatus = Column("suspension_status", Enum(SuspensionStatus), nullable=False)
    """
    The status of suspension for a participant.

    :ref:`Enumerated values <suspension_status>`
    """

    suspensionTime = Column("suspension_time", UTCDateTime)
    "The time that the API set the participant as suspended"


    # The originating resource for participant, this (unlike clientId) will not change.
    participantOrigin = Column("participant_origin", String(80), nullable=False)
    "The sign up portal the participant used to enroll (Vibrent, Care Evolution)."

    # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
    # # patientStatuses = relationship("PatientStatus", back_populates="participantSummary")
    # patientStatus = relationship(
    #   "PatientStatus",
    #   primaryjoin="PatientStatus.participantId == ParticipantSummary.participantId",
    #   foreign_keys=participantId,
    #   remote_side="PatientStatus.participantId",
    #   viewonly=True,
    #   uselist=True
    # )
    patientStatus = Column("patient_status", JSON, nullable=True, default=list())
    """
    A flag available for sites of in person enrollment. A participant can have a status from multiple sites.  Example:

    .. code-block:: json

        "patientStatus": {
            “PITT_UPMC”: “YES”,
            “PITT_TEMPLE”: “NO_ACCESS”,
            “PITT_SOMETHING”: “NO”
        }

    .. note::
        The following values are available.

        * Yes: Confirmed in EHR system.
        * No: Not found in EHR system.
        * No Access: Unable to check EHR system.
        * Unknown: Inconclusive search results.
        * Not Applicable (will apply to DVs only).
    """

    # Fields for which questionnaires have been submitted, and at what times.
    consentForStudyEnrollment = Column(
        "consent_for_study_enrollment", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates whether enrollment consent has been received

    :ref:`Enumerated values <questionnaire_status>`
    """

    consentForStudyEnrollmentTime = Column("consent_for_study_enrollment_time", UTCDateTime)
    "UTC timestamp indicating the time at which enrollment consent has been received (ISO-8601 time)"

    consentForStudyEnrollmentAuthored = Column("consent_for_study_enrollment_authored", UTCDateTime)
    "The UTC date time of the latest time participant completed the survey, regardless of when it was sent to RDR"

    consentForStudyEnrollmentFirstYesAuthored = Column("consent_for_study_enrollment_first_yes_authored", UTCDateTime)
    "The UTC date time of the first time the participant completed the survey, regardless of when it was sent to RDR"

    semanticVersionForPrimaryConsent = Column("semantic_version_for_primary_consent", String(100))
    "The human readable version of primary consent the participant signed"

    consentForElectronicHealthRecords = Column(
        "consent_for_electronic_health_records", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates whether electronic health records (EHR) consent has been received

    :ref:`Enumerated values <questionnaire_status>`
    """

    consentForElectronicHealthRecordsTime = Column("consent_for_electronic_health_records_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of consentForElectronicHealthRecords."

    consentForElectronicHealthRecordsAuthored = Column("consent_for_electronic_health_records_authored", UTCDateTime)
    "Indicates the latest time at which the participant completed consent, regardless of when it was sent to RDR."

    consentForElectronicHealthRecordsFirstYesAuthored = Column(
        "consent_for_electronic_health_records_first_yes_authored",
        UTCDateTime
    )
    "Indicates the first time at which the participant consented with Yes"

    ehrConsentExpireStatus = Column("ehr_consent_expire_status", Enum(ConsentExpireStatus),
                                    default=ConsentExpireStatus.UNSET)
    """
    Indicates whether the EHR consent has expired per rules for impacted states (Maine, Maryland, Montana, Wyoming)
    For HPO participants, the EHR consent is based on the HPO state and not the participant's residence.

    :ref:`Enumerated values <ehr_consent_expire_status>`
    """
    ehrConsentExpireTime = Column("ehr_consent_expire_time", UTCDateTime)
    """
    Indicates the time at which the RDR received notice of the EHR consent expiration
    which may be subject to expiration in certain states
    """

    ehrConsentExpireAuthored = Column("ehr_consent_expire_authored", UTCDateTime)
    """
    Indicates the time at which the participant completed an EHR consent
    which may be subject to expiration in certain states
    """

    consentForDvElectronicHealthRecordsSharing = Column(
        "consent_for_dv_electronic_health_records_sharing",
        Enum(QuestionnaireStatus),
        default=QuestionnaireStatus.UNSET,
    )
    """
    Indicates whether direct-volunteer electronic health record sharing consent has been received

    :ref:`Enumerated values <questionnaire_status>`
    """

    consentForDvElectronicHealthRecordsSharingTime = Column(
        "consent_for_dv_electronic_health_records_sharing_time", UTCDateTime
    )
    "Indicates the time at which the RDR received notice of consentForDvElectronicHealthRecordsSharing"

    consentForDvElectronicHealthRecordsSharingAuthored = Column(
        "consent_for_dv_electronic_health_records_sharing_authored", UTCDateTime
    )
    "Indicates the time at which the participant completed consent, regardless of when it was sent to RDR"

    consentForGenomicsROR = Column("consent_for_genomics_ror", Enum(QuestionnaireStatus),
                                   default=QuestionnaireStatus.UNSET)
    """
    Indicates whether genomic return of results consent has been received

    :ref:`Enumerated values <questionnaire_status>`
    """

    consentForGenomicsRORTime = Column("consent_for_genomics_ror_time", UTCDateTime)
    "Indicates the time the genomic return of results was received by the RDR"

    consentForGenomicsRORAuthored = Column("consent_for_genomics_ror_authored", UTCDateTime)
    "Indicates the time the participant signed the genomic return of results"

    consentForCABoR = Column("consent_for_cabor", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET)
    """
    Indicates whether California Bill of Rights (Bor) consent has been received

    :ref:`Enumerated values <questionnaire_status>`
    """

    consentForCABoRTime = Column("consent_for_cabor_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of consentForCABoR"

    consentForCABoRAuthored = Column("consent_for_cabor_authored", UTCDateTime)
    """
    Indicates the time at which the participant completed California Bill of Rights consent,
    regardless of when it was sent to RDR
    """

    questionnaireOnOverallHealth = Column(
        "questionnaire_on_overall_health", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates status for Overall Health PPI module

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnOverallHealthTime = Column("questionnaire_on_overall_health_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of overall health questionnaire"

    questionnaireOnOverallHealthAuthored = Column("questionnaire_on_overall_health_authored", UTCDateTime)
    "Indicates the time at which the participant completed the overall health questionnaire"

    questionnaireOnLifestyle = Column(
        "questionnaire_on_lifestyle", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates the status of a questionnaire on lifestyle

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnLifestyleTime = Column("questionnaire_on_lifestyle_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of lifestyle questionnaire"

    questionnaireOnLifestyleAuthored = Column("questionnaire_on_lifestyle_authored", UTCDateTime)
    "Indicates the time at which the participant completed the lifestyle questionnaire"

    questionnaireOnTheBasics = Column(
        "questionnaire_on_the_basics", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates the status of a questionnaire on TheBasics that a participant can fill out.

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnTheBasicsTime = Column("questionnaire_on_the_basics_time", UTCDateTime)
    "The UTC Date time of when the RDR received the basics questionnaire."

    questionnaireOnTheBasicsAuthored = Column("questionnaire_on_the_basics_authored", UTCDateTime)
    "The UTC Date time of when the participant completed the basics questionnaire."

    baselineQuestionnairesFirstCompleteAuthored = Column("baseline_questionnaires_first_complete_authored", UTCDateTime)
    "The UTC Date time of when the participant first time completed all the baseline questionnaire."

    questionnaireOnHealthcareAccess = Column(
        "questionnaire_on_healthcare_access", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates status of a questionnaire on HealthcareAccess that a participant can fill out.

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnHealthcareAccessTime = Column("questionnaire_on_healthcare_access_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of health care access questionnaire."

    questionnaireOnHealthcareAccessAuthored = Column("questionnaire_on_healthcare_access_authored", UTCDateTime)
    "Indicates the time at which the participant completed the health care access questionnaire."

    questionnaireOnMedicalHistory = Column(
        "questionnaire_on_medical_history", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates the status of a questionnaire on MedicalHistory that a participant can fill out.

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnMedicalHistoryTime = Column("questionnaire_on_medical_history_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of medical history questionnaire"

    questionnaireOnMedicalHistoryAuthored = Column("questionnaire_on_medical_history_authored", UTCDateTime)
    "Indicates the time at which the participant completed the medical history questionnaire"

    questionnaireOnMedications = Column(
        "questionnaire_on_medications", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates status of a questionnaire on Medications that a participant can fill out.

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnMedicationsTime = Column("questionnaire_on_medications_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of medications questionnaire."

    questionnaireOnMedicationsAuthored = Column("questionnaire_on_medications_authored", UTCDateTime)
    "Indicates the time at which the participant completed the medications questionnaire."

    questionnaireOnFamilyHealth = Column(
        "questionnaire_on_family_health", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates the status of a questionnaire on FamilyHealth that a participant can fill out.

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnFamilyHealthTime = Column("questionnaire_on_family_health_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of family health questionnaire."

    questionnaireOnFamilyHealthAuthored = Column("questionnaire_on_family_health_authored", UTCDateTime)
    "Indicates the time at which the participant completed the family health questionnaire."

    questionnaireOnCopeMay = Column(
        "questionnaire_on_cope_may", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    questionnaireOnCopeJune = Column(
        "questionnaire_on_cope_june", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    questionnaireOnCopeJuly = Column(
        "questionnaire_on_cope_july", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    questionnaireOnCopeNov = Column(
        "questionnaire_on_cope_nov", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    questionnaireOnCopeDec = Column(
        "questionnaire_on_cope_dec", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates the status of a periodic questionnaire on COVID Participant Experience (COPE)
    that a participant can fill out.

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnCopeMayTime = Column("questionnaire_on_cope_may_time", UTCDateTime)
    questionnaireOnCopeJuneTime = Column("questionnaire_on_cope_june_time", UTCDateTime)
    questionnaireOnCopeJulyTime = Column("questionnaire_on_cope_july_time", UTCDateTime)
    questionnaireOnCopeNovTime = Column("questionnaire_on_cope_nov_time", UTCDateTime)
    questionnaireOnCopeDecTime = Column("questionnaire_on_cope_dec_time", UTCDateTime)
    "Indicates the time at which the RDR received notice of the specified COPE questionnaire."

    questionnaireOnCopeMayAuthored = Column("questionnaire_on_cope_may_authored", UTCDateTime)
    questionnaireOnCopeJuneAuthored = Column("questionnaire_on_cope_june_authored", UTCDateTime)
    questionnaireOnCopeJulyAuthored = Column("questionnaire_on_cope_july_authored", UTCDateTime)
    questionnaireOnCopeNovAuthored = Column("questionnaire_on_cope_nov_authored", UTCDateTime)
    questionnaireOnCopeDecAuthored = Column("questionnaire_on_cope_dec_authored", UTCDateTime)
    "Indicates the time at which the participant completed the specified COPE questionnaire."

    questionnaireOnDnaProgram = Column(
        "questionnaire_on_dna_program", Enum(QuestionnaireStatus), default=QuestionnaireStatus.UNSET
    )
    """
    Indicates the status of a questionnaire of the DNA program that a participant can fill out

    :ref:`Enumerated values <questionnaire_status>`
    """

    questionnaireOnDnaProgramAuthored = Column("questionnaire_on_dna_program_authored", UTCDateTime)
    "The UTC Date time of when the participant completed the DNA program questionnaire"

    numCompletedBaselinePPIModules = Column("num_completed_baseline_ppi_modules", SmallInteger, default=0)
    """
    The count of how many of [questionnaireOnTheBasics, questionnaireOnOverallHealth, questionnaireOnLifestyle]
    the participant has completed.
    """
    numCompletedPPIModules = Column("num_completed_ppi_modules", SmallInteger, default=0)
    "The count of all PPI modules the participant has completed."

    biospecimenStatus = Column("biospecimen_status", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    Whether biospecimens have been finalized for the participant.

    :ref:`Enumerated values <biospecimen_status>`
    """
    biospecimenOrderTime = Column("biospecimen_order_time", UTCDateTime)
    "The first time at which biospecimens were finalized in UTC."

    biospecimenSourceSiteId = Column("biospecimen_source_site_id", Integer, ForeignKey("site.site_id"))
    biospecimenSourceSite = None  # placeholder for docs, API sets on model using corresponding ID field
    "The site where biospecimens were initially created for the participant"

    biospecimenCollectedSiteId = Column("biospecimen_collected_site_id", Integer, ForeignKey("site.site_id"))
    biospecimenCollectedSite = None  # placeholder for docs, API sets on model using corresponding ID field
    "The site where biospecimens were initially collected for the participant"

    biospecimenProcessedSiteId = Column("biospecimen_processed_site_id", Integer, ForeignKey("site.site_id"))
    biospecimenProcessedSite = None  # placeholder for docs, API sets on model using corresponding ID field
    "The site where biospecimens were initially processed for the participant"

    biospecimenFinalizedSiteId = Column("biospecimen_finalized_site_id", Integer, ForeignKey("site.site_id"))
    biospecimenFinalizedSite = None  # placeholder for docs, API sets on model using corresponding ID field
    "The site where biospecimens were initially finalized for the participant"

    # Fields for which samples have been received, and at what times.
    sampleStatus1SST8 = Column("sample_status_1sst8", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1SST8.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1SST8Time = Column("sample_status_1sst8_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus2SST8 = Column("sample_status_2sst8", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 2SST8.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus2SST8Time = Column("sample_status_2sst8_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1SS08 = Column("sample_status_1ss08", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1SS08.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1SS08Time = Column("sample_status_1ss08_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1PST8 = Column("sample_status_1pst8", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1PST8.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1PST8Time = Column("sample_status_1pst8_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus2PST8 = Column("sample_status_2pst8", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 2PST8.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus2PST8Time = Column("sample_status_2pst8_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1PS08 = Column("sample_status_1ps08", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1PS08.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1PS08Time = Column("sample_status_1ps08_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1HEP4 = Column("sample_status_1hep4", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1HEP4.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1HEP4Time = Column("sample_status_1hep4_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1ED04 = Column("sample_status_1ed04", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1ED04.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1ED04Time = Column("sample_status_1ed04_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1ED10 = Column("sample_status_1ed10", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1ED10.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1ED10Time = Column("sample_status_1ed10_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus2ED10 = Column("sample_status_2ed10", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 2ED10.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus2ED10Time = Column("sample_status_2ed10_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1UR10 = Column("sample_status_1ur10", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1UR10.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1UR10Time = Column("sample_status_1ur10_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1UR90 = Column("sample_status_1ur90", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1UR90.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1UR90Time = Column("sample_status_1ur90_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1SAL = Column("sample_status_1sal", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1SAL.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1SALTime = Column("sample_status_1sal_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1SAL2 = Column("sample_status_1sal2", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1SAL2.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1SAL2Time = Column("sample_status_1sal2_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sample1SAL2CollectionMethod = Column(
        "sample_1sal2_collection_method",
        Enum(SampleCollectionMethod),
        default=SampleCollectionMethod.UNSET
    )
    """
    Gives how the 1SAL2 sample was collected (ie on site or using a mail kit)

    :ref:`Enumerated values <Sample_collection_method>`
    """

    sampleStatus1ED02 = Column("sample_status_1ed02", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1ED02.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1ED02Time = Column("sample_status_1ed02_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1CFD9 = Column("sample_status_1cfd9", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1CFD9.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1CFD9Time = Column("sample_status_1cfd9_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleStatus1PXR2 = Column("sample_status_1pxr2", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample 1PXR2.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatus1PXR2Time = Column("sample_status_1pxr2_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."


    # Sample fields for Direct Volunteers
    # These are deprecated in favor of using the standard samplestatus2sal2, etc.
    sampleStatusDV1SAL2 = Column("sample_status_dv_1sal2", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The result of biobank processing on sample DV1SAL2.

    :ref:`Enumerated values <sample_status>`
    """

    sampleStatusDV1SAL2Time = Column("sample_status_dv_1sal2_time", UTCDateTime)
    "The datetime in UTC in which the biobank processed the sample."

    sampleOrderStatusDV1SAL2 = Column("sample_order_status_dv_1sal2", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample DV1SAL2.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatusDV1SAL2Time = Column("sample_order_status_dv_1sal2_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1SST8 = Column("sample_order_status_1sst8", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1SST8.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1SST8Time = Column("sample_order_status_1sst8_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus2SST8 = Column("sample_order_status_2sst8", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 2SST8.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus2SST8Time = Column("sample_order_status_2sst8_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1SS08 = Column("sample_order_status_1ss08", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1SS08.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1SS08Time = Column("sample_order_status_1ss08_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1PST8 = Column("sample_order_status_1pst8", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1PST8.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1PST8Time = Column("sample_order_status_1pst8_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus2PST8 = Column("sample_order_status_2pst8", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 2PST8.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus2PST8Time = Column("sample_order_status_2pst8_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1PS08 = Column("sample_order_status_1ps08", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1PS08.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1PS08Time = Column("sample_order_status_1ps08_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1HEP4 = Column("sample_order_status_1hep4", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1HEP4.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1HEP4Time = Column("sample_order_status_1hep4_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1ED04 = Column("sample_order_status_1ed04", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1ED04.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1ED04Time = Column("sample_order_status_1ed04_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1ED10 = Column("sample_order_status_1ed10", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1ED10.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1ED10Time = Column("sample_order_status_1ed10_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus2ED10 = Column("sample_order_status_2ed10", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 2ED10.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus2ED10Time = Column("sample_order_status_2ed10_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1UR10 = Column("sample_order_status_1ur10", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1UR10.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1UR10Time = Column("sample_order_status_1ur10_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1UR90 = Column("sample_order_status_1ur90", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1UR90.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1UR90Time = Column("sample_order_status_1ur90_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1SAL = Column("sample_order_status_1sal", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1SAL.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1SALTime = Column("sample_order_status_1sal_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1SAL2 = Column("sample_order_status_1sal2", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1SAL2.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1SAL2Time = Column("sample_order_status_1sal2_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1ED02 = Column("sample_order_status_1ed02", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1ED02.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1ED02Time = Column("sample_order_status_1ed02_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1CFD9 = Column("sample_order_status_1cfd9", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1CFD9.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1CFD9Time = Column("sample_order_status_1cfd9_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."

    sampleOrderStatus1PXR2 = Column("sample_order_status_1pxr2", Enum(OrderStatus), default=OrderStatus.UNSET)
    """
    The individual order status of sample 1PXR2.

    :ref:`Enumerated values <sample_order_status>`
    """

    sampleOrderStatus1PXR2Time = Column("sample_order_status_1pxr2_time", UTCDateTime)
    "The time the sample was marked as finalized by the processing site."


    # The number of BiobankStoredSamples recorded for this participant, limited to those samples
    # where testCode is one of the baseline tests (listed in the config).
    numBaselineSamplesArrived = Column("num_baseline_samples_arrived", SmallInteger, default=0)
    """
    The count of samples the biobank has recorded from baseline sample list

      * 1ED04
      * 1ED10
      * 1HEP4
      * 1PST8
      * 2PST8
      * 1SST8
      * 2SST8
      * 1PS08
      * 1SS08
      * 1UR10
      * 1CFD9
      * 1PXR2
      * 1UR90
      * 2ED10
    """

    samplesToIsolateDNA = Column("samples_to_isolate_dna", Enum(SampleStatus), default=SampleStatus.UNSET)
    """
    The sample status of any dna retrievable samples ordered for participant.

    :ref:`Enumerated values <sample_status>`

    DNA sample test codes

    * 1ED10
    * 2ED10
    * 1ED04
    * 1SAL
    * 1SAL2
    """

    participant = relationship("Participant", back_populates="participantSummary")

    retentionEligibleStatus = Column(
        "retention_eligible_status",
        Enum(RetentionStatus),
        Computed(_COMPUTE_RETENTION_ELIGIBLE_SQL, persisted=True)
    )
    """
    A participant is considered eligible for retention if they have completed all of the following:

    * Primary consent
    * Is consented yes to EHR consent OR DV EHR attestation
    * Completed the Basics, Lifestyle, and Overall Health PPI modules
    * Provided a blood or saliva sample for DNA that has been received by the Biobank
    * IS NOT withdrawn, deceased, or deactivated
    * Completed PPI 1-3 (Basics, Lifestyle, and Overall Health)

    :ref:`Enumerated values <retention_status>`
    """

    retentionEligibleTime = Column(
        "retention_eligible_time", UTCDateTime, Computed(_COMPUTE_RETENTION_ELIGIBLE_TIME_SQL, persisted=True)
    )
    """
    Present if a participant is retention eligible.

    Is the latest date from the list of:

    * The earliest date of sampleStatus...Time (any of the DNA sample tests)
    * consentForStudyEnrollmentAuthored
    * questionnaireOnTheBasicsAuthored
    * questionnaireOnOverallHealthAuthored
    * questionnaireOnLifestyleAuthored
    * consentForElectronicHealthRecordsAuthored
    * consentForDvElectronicHealthRecordsSharingAuthored
    """

    lastModified = Column("last_modified", UTCDateTime6)
    "UTC timestamp of the last time the participant summary was modified"


Index("participant_summary_biobank_id", ParticipantSummary.biobankId)
Index("participant_summary_ln_dob", ParticipantSummary.lastName, ParticipantSummary.dateOfBirth)
Index(
    "participant_summary_ln_dob_zip",
    ParticipantSummary.lastName,
    ParticipantSummary.dateOfBirth,
    ParticipantSummary.zipCode,
)
Index(
    "participant_summary_ln_dob_fn",
    ParticipantSummary.lastName,
    ParticipantSummary.dateOfBirth,
    ParticipantSummary.firstName,
)
Index("participant_summary_hpo", ParticipantSummary.hpoId)
Index("participant_summary_hpo_fn", ParticipantSummary.hpoId, ParticipantSummary.firstName)
Index("participant_summary_hpo_ln", ParticipantSummary.hpoId, ParticipantSummary.lastName)
Index("participant_summary_hpo_dob", ParticipantSummary.hpoId, ParticipantSummary.dateOfBirth)
Index("participant_summary_hpo_race", ParticipantSummary.hpoId, ParticipantSummary.race)
Index("participant_summary_hpo_zip", ParticipantSummary.hpoId, ParticipantSummary.zipCode)
Index("participant_summary_hpo_status", ParticipantSummary.hpoId, ParticipantSummary.enrollmentStatus)
Index("participant_summary_hpo_consent", ParticipantSummary.hpoId, ParticipantSummary.consentForStudyEnrollment)
Index(
    "participant_summary_hpo_num_baseline_ppi",
    ParticipantSummary.hpoId,
    ParticipantSummary.numCompletedBaselinePPIModules,
)
Index(
    "participant_summary_hpo_num_baseline_samples",
    ParticipantSummary.hpoId,
    ParticipantSummary.numBaselineSamplesArrived,
)
Index(
    "participant_summary_hpo_withdrawal_status_time",
    ParticipantSummary.hpoId,
    ParticipantSummary.withdrawalStatus,
    ParticipantSummary.withdrawalTime,
)
Index("participant_summary_last_modified", ParticipantSummary.hpoId, ParticipantSummary.lastModified)


class ParticipantGenderAnswers(Base):
    __tablename__ = "participant_gender_answers"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), autoincrement=False)
    created = Column("created", DateTime, nullable=True)
    modified = Column("modified", DateTime, nullable=True)
    codeId = Column("code_id", Integer, ForeignKey("code.code_id"), nullable=False)


event.listen(ParticipantGenderAnswers, "before_insert", model_insert_listener)
event.listen(ParticipantGenderAnswers, "before_update", model_update_listener)


class ParticipantRaceAnswers(Base):
    __tablename__ = "participant_race_answers"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), autoincrement=False)
    created = Column("created", DateTime, nullable=True)
    modified = Column("modified", DateTime, nullable=True)
    codeId = Column("code_id", Integer, ForeignKey("code.code_id"), nullable=False)


event.listen(ParticipantRaceAnswers, "before_insert", model_insert_listener)
event.listen(ParticipantRaceAnswers, "before_update", model_update_listener)
