import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from protorpc import messages

from rdr_service.code_constants import (
    GENDER_MAN_CODE,
    GENDER_NONBINARY_CODE,
    GENDER_NO_GENDER_IDENTITY_CODE,
    GENDER_OTHER_CODE,
    GENDER_TRANSGENDER_CODE,
    GENDER_WOMAN_CODE,
    PMI_FREE_TEXT_CODE,
    PMI_OTHER_CODE,
    PMI_PREFER_NOT_TO_ANSWER_CODE,
    PMI_SKIP_CODE,
    PMI_UNANSWERED_CODE,
    RACE_AIAN_CODE,
    RACE_ASIAN_CODE,
    RACE_BLACK_CODE,
    RACE_FREETEXT_CODE,
    RACE_HISPANIC_CODE,
    RACE_MENA_CODE,
    RACE_NHDPI_CODE,
    RACE_NONE_OF_THESE_CODE,
    RACE_WHITE_CODE,
    UNSET,
)  # Internal Use Codes; PMI Codes; Race Codes; Gender Codes

# These are handled specially in code; others will be inserted into the database and handled
# dynamically.
UNSET_HPO_ID = 0

# A pattern for test participant email addresses.
TEST_EMAIL_PATTERN = "%@example.com"
# The name of the 'test' HPO that test participants are normally affiliated with.
TEST_HPO_NAME = "TEST"
TEST_HPO_ID = 21
# Test login phone number prefix
TEST_LOGIN_PHONE_NUMBER_PREFIX = "4442"
PARTICIPANT_COHORT_2_START_TIME = datetime(2018, 4, 24, 0, 0, 0)
PARTICIPANT_COHORT_3_START_TIME = datetime(2020, 4, 21, 4, 0, 0)


class ParticipantCohort(messages.Enum):
    """ Participant Cohort Group"""
    UNSET = 0
    COHORT_1 = 1  # Beta participants.  Consent before April 24, 2018.
    COHORT_2 = 2  # National Launch Participants. Consent between April 24, 2018 and April 21, 2020 (03:59:59 UTC)
    COHORT_3 = 3  # New Participants with consent starting from April 21, 2020 04:00:00 UTC  (midnight eastern)


# Added for DA-1622, enabling identification of Genomics pilot participants from Cohort 2
class ParticipantCohortPilotFlag(messages.Enum):
    """ Participant Cohort Pilot Group """
    UNSET = 0
    COHORT_2_PILOT = 1 # Genomics Cohort 2 Pilot Group


class PatientStatusFlag(messages.Enum):
    """Site patient status"""

    UNSET = 0
    NO = 1
    YES = 2
    NO_ACCESS = 3
    UNKNOWN = 4


class PhysicalMeasurementsStatus(messages.Enum):
    """The state of the participant's physical measurements."""

    UNSET = 0
    COMPLETED = 1
    CANCELLED = 2


class PhysicalMeasurementsCollectType(messages.Enum):
    """The collect type of the participant's physical measurements"""

    UNSET = 0
    SITE = 1
    SELF_REPORTED = 2


class SelfReportedPhysicalMeasurementsStatus(messages.Enum):
    """The state of the participants self-reported physical measurements"""

    UNSET = 0
    COMPLETED = 1


class OriginMeasurementUnit(messages.Enum):
    """The origin unit type of this measurement record"""
    UNSET = 0
    IMPERIAL = 1
    METRIC = 2


class QuestionnaireStatus(messages.Enum):
    """The status of a given questionnaire for this participant"""

    UNSET = 0
    SUBMITTED = 1
    SUBMITTED_NO_CONSENT = 2
    SUBMITTED_NOT_SURE = 3
    SUBMITTED_INVALID = 4


class QuestionnaireDefinitionStatus(messages.Enum):
    """ If a questionnaire has been determined to be invalid"""

    VALID = 0
    INVALID = 1


class QuestionnaireResponseStatus(messages.Enum):
    """Given status of a questionnaire response"""

    IN_PROGRESS = 0
    COMPLETED = 1
    AMENDED = 2
    ENTERED_IN_ERROR = 3
    STOPPED = 4


class QuestionnaireResponseClassificationType(messages.Enum):
    """
    Categorize questionnaire response payloads exhibiting known data issues
    See:  DA-2192 and the linked investigation document for more details
    """
    COMPLETE = 0               # Default, no known issues / normal completed survey payload
    DUPLICATE = 1              # Identical answer hash to another response or has a cascading subset/superset signature
    PROFILE_UPDATE = 2         # E.g., TheBasics response payloads which only contain secondary contact updates
    NO_ANSWER_VALUES = 3       # Isolated cases where payload had question data with no answer values
    AUTHORED_TIME_UPDATED = 4  # Known/expected retransmission of previous payloads, but with a corrected authored ts
    PARTIAL = 5                # Other cases (e.g., partial COPE surveys) where payload is not a completed survey


class EnrollmentStatus(messages.Enum):
    """A status reflecting how fully enrolled a participant is"""

    # REGISTERED should not be added here as doing so will break metric counts over time.
    INTERESTED = 1
    MEMBER = 2
    FULL_PARTICIPANT = 3
    CORE_MINUS_PM = 4


class EnrollmentStatusV2(messages.Enum):
    """A status reflecting how fully enrolled a participant is(Version 2)"""

    REGISTERED = 0
    PARTICIPANT = 1
    FULLY_CONSENTED = 2
    CORE_PARTICIPANT = 3
    CORE_MINUS_PM = 4


class EnrollmentStatusV30(messages.Enum):
    """A status reflecting how fully enrolled a participant is according to the 3.0 data glossary"""

    PARTICIPANT = 1
    PARTICIPANT_PLUS_EHR = 2
    PARTICIPANT_PMB_ELIGIBLE = 3
    CORE_MINUS_PM = 4
    CORE_PARTICIPANT = 5


class EnrollmentStatusV31(messages.Enum):
    """A status reflecting how fully enrolled a participant is according to the 3.1 data glossary"""

    PARTICIPANT = 1
    PARTICIPANT_PLUS_EHR = 2
    PARTICIPANT_PLUS_BASICS = 3
    CORE_MINUS_PM = 4
    CORE_PARTICIPANT = 5
    BASELINE_PARTICIPANT = 6


class DigitalHealthSharingStatusV31(messages.Enum):
    """Provides whether EHR files have been or currently are available for the participant"""

    NEVER_SHARED = 1
    EVER_SHARED = 2
    CURRENTLY_SHARING = 3


class SampleStatus(messages.Enum):
    """Status of biobank samples"""

    UNSET = 0
    RECEIVED = 1
    # DA-814 - sample disposal statuses for a good outcome.
    DISPOSED = 10
    CONSUMED = 11
    UNKNOWN = 12
    # DA-814 - sample disposal statuses for a bad outcome.
    SAMPLE_NOT_RECEIVED = 13
    SAMPLE_NOT_PROCESSED = 14
    ACCESSINGING_ERROR = 15
    LAB_ACCIDENT = 16
    QNS_FOR_PROCESSING = 17
    QUALITY_ISSUE = 18


class SampleCollectionMethod(messages.Enum):
    """How a sample was collected"""

    UNSET = 0
    MAIL_KIT = 1
    ON_SITE = 2


class EhrStatus(messages.Enum):
    """Status of EHRs"""

    NOT_PRESENT = 0
    PRESENT = 1


def get_sample_status_enum_value(status):
    """
  Return the SampleStatus enum value for the given status from Mayo
  :param status: a sample status value
  :return: SampleStatus enum value
  """
    if status is None:
        return SampleStatus.UNSET

    # Set Received if we have an empty status value, the sample has not been disposed of yet.
    if not status:
        return SampleStatus.RECEIVED

    status = status.lower()

    if status == "disposed":
        return SampleStatus.DISPOSED
    elif status == "consumed":
        return SampleStatus.CONSUMED
    elif status == "sample not received":
        return SampleStatus.SAMPLE_NOT_RECEIVED
    elif status == "sample not processed":
        return SampleStatus.SAMPLE_NOT_PROCESSED
    elif status == "accessioning error":
        return SampleStatus.ACCESSINGING_ERROR
    elif status == "lab accident":
        return SampleStatus.LAB_ACCIDENT
    elif status == "qns for processing":
        return SampleStatus.QNS_FOR_PROCESSING
    elif status == "quality issue":
        return SampleStatus.QUALITY_ISSUE

    # Set unknown for any new status values.
    return SampleStatus.UNKNOWN


class OrderStatus(messages.Enum):
    """Status of biobank orders and samples"""

    UNSET = 0
    CREATED = 1
    COLLECTED = 2
    PROCESSED = 3
    FINALIZED = 4


class OrderShipmentStatus(messages.Enum):
    """Shipment Status of biobank order for mail-in orders"""

    UNSET = 0
    PENDING = 1
    QUEUED = 2
    FULFILLMENT = 3
    SHIPPED = 4
    ERROR = 5


class OrderShipmentTrackingStatus(messages.Enum):
    """ Shipment tracking status of biobank order for mail-in orders"""

    UNSET = 0
    IN_TRANSIT = 1
    DELIVERED = 2


class BiobankOrderStatus(messages.Enum):
    """ The status of a biobank order: amended/cancelled """

    UNSET = 0
    AMENDED = 1
    CANCELLED = 2


class MetricSetType(messages.Enum):
    """Type determining the schema for a metric set."""

    PUBLIC_PARTICIPANT_AGGREGATIONS = 1


class MetricsKey(messages.Enum):
    """Key for a metrics set metric aggregation."""

    GENDER = 1
    RACE = 2
    STATE = 3
    AGE_RANGE = 4
    PHYSICAL_MEASUREMENTS = 5
    BIOSPECIMEN_SAMPLES = 6
    QUESTIONNAIRE_ON_OVERALL_HEALTH = 7
    QUESTIONNAIRE_ON_PERSONAL_HABITS = 8
    QUESTIONNAIRE_ON_SOCIODEMOGRAPHICS = 9
    ENROLLMENT_STATUS = 10


class Stratifications(messages.Enum):
    """Variables by which participant counts can be stacked"""

    TOTAL = 1
    ENROLLMENT_STATUS = 2
    GENDER_IDENTITY = 3
    RACE = 4
    AGE_RANGE = 5
    EHR_CONSENT = 6
    EHR_RATIO = 7
    FULL_STATE = 8
    FULL_CENSUS = 9
    FULL_AWARDEE = 10
    LIFECYCLE = 11
    GEO_STATE = 12
    GEO_CENSUS = 13
    GEO_AWARDEE = 14
    LANGUAGE = 15
    PRIMARY_CONSENT = 16
    EHR_METRICS = 17
    SITES_COUNT = 18
    PARTICIPANT_ORIGIN = 19


METRIC_SET_KEYS = {
    MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS: set(
        [
            MetricsKey.GENDER,
            MetricsKey.RACE,
            MetricsKey.STATE,
            MetricsKey.AGE_RANGE,
            MetricsKey.PHYSICAL_MEASUREMENTS,
            MetricsKey.BIOSPECIMEN_SAMPLES,
            MetricsKey.QUESTIONNAIRE_ON_OVERALL_HEALTH,
            MetricsKey.QUESTIONNAIRE_ON_PERSONAL_HABITS,
            MetricsKey.QUESTIONNAIRE_ON_SOCIODEMOGRAPHICS,
            MetricsKey.ENROLLMENT_STATUS,
        ]
    )
}


# These race values are derived from one or more answers to the race/ethnicity question
# in questionnaire responses.
class Race(messages.Enum):
    UNSET = 0
    PMI_Skip = 1
    # UNMAPPED = 2 -- Not actually in use.
    AMERICAN_INDIAN_OR_ALASKA_NATIVE = 3
    BLACK_OR_AFRICAN_AMERICAN = 4
    ASIAN = 5
    NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = 6
    WHITE = 7
    HISPANIC_LATINO_OR_SPANISH = 8
    MIDDLE_EASTERN_OR_NORTH_AFRICAN = 9
    HLS_AND_WHITE = 10
    HLS_AND_BLACK = 11
    HLS_AND_ONE_OTHER_RACE = 12
    HLS_AND_MORE_THAN_ONE_OTHER_RACE = 13
    MORE_THAN_ONE_RACE = 14
    OTHER_RACE = 15
    PREFER_NOT_TO_SAY = 16


class GenderIdentity(messages.Enum):
    UNSET = 0
    PMI_Skip = 1
    GenderIdentity_Man = 2
    GenderIdentity_Woman = 3
    GenderIdentity_NonBinary = 4
    GenderIdentity_Transgender = 5
    GenderIdentity_AdditionalOptions = 6
    GenderIdentity_MoreThanOne = 7
    PMI_PreferNotToAnswer = 8


# A type of organization responsible for signing up participants.
class OrganizationType(messages.Enum):
    UNSET = 0
    # Healthcare Provider Organization
    HPO = 1
    # Federally Qualified Health Center
    FQHC = 2
    # Direct Volunteer Recruitment Center
    DV = 3
    # Veterans Administration
    VA = 4


ANSWER_CODE_TO_RACE = {
    RACE_AIAN_CODE: Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE,
    RACE_ASIAN_CODE: Race.ASIAN,
    RACE_BLACK_CODE: Race.BLACK_OR_AFRICAN_AMERICAN,
    RACE_MENA_CODE: Race.MIDDLE_EASTERN_OR_NORTH_AFRICAN,
    RACE_NHDPI_CODE: Race.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER,
    RACE_WHITE_CODE: Race.WHITE,
    RACE_HISPANIC_CODE: Race.HISPANIC_LATINO_OR_SPANISH,
    RACE_FREETEXT_CODE: Race.OTHER_RACE,
    PMI_PREFER_NOT_TO_ANSWER_CODE: Race.PREFER_NOT_TO_SAY,
    RACE_NONE_OF_THESE_CODE: Race.OTHER_RACE,
    PMI_OTHER_CODE: Race.OTHER_RACE,
    PMI_FREE_TEXT_CODE: Race.OTHER_RACE,
    PMI_UNANSWERED_CODE: Race.UNSET,
    PMI_SKIP_CODE: Race.PMI_Skip,
}

ANSWER_CODE_TO_GENDER = {
    GENDER_MAN_CODE: GenderIdentity.GenderIdentity_Man,
    GENDER_WOMAN_CODE: GenderIdentity.GenderIdentity_Woman,
    GENDER_NONBINARY_CODE: GenderIdentity.GenderIdentity_NonBinary,
    GENDER_TRANSGENDER_CODE: GenderIdentity.GenderIdentity_Transgender,
    GENDER_OTHER_CODE: GenderIdentity.GenderIdentity_AdditionalOptions,
    PMI_PREFER_NOT_TO_ANSWER_CODE: GenderIdentity.PMI_PreferNotToAnswer,
    GENDER_NO_GENDER_IDENTITY_CODE: GenderIdentity.PMI_Skip,
    PMI_SKIP_CODE: GenderIdentity.PMI_Skip,
}


class WithdrawalStatus(messages.Enum):
    """Whether a participant has withdrawn from the study."""

    NOT_WITHDRAWN = 1
    NO_USE = 2
    EARLY_OUT = 3


class WithdrawalReason(messages.Enum):
    """Whether a participant has been administratively withdrawn from the study.
     If value is UNSET it mean that a participant went through the normal withdrawal process."""

    UNSET = 0
    FRAUDULENT = 1
    DUPLICATE = 2
    TEST = 3

# PDR-252:  This information will initially be required in PDR data for providing metrics.  There are no
# initial requirements to include this information in RDR GET API responses
class WithdrawalAIANCeremonyStatus(messages.Enum):
    """Whether an AIAN participant requested a last rites ceremony for their samples when withdrawing.
     UNSET indicates no response exists (question did not apply or AIAN participant never submitted a valid response)"""

    UNSET = 0
    DECLINED = 1
    REQUESTED = 2


class ConsentExpireStatus(messages.Enum):
    UNSET = 0
    NOT_EXPIRED = 1
    EXPIRED = 2


class SuspensionStatus(messages.Enum):
    """Whether a participant has been suspended from the study."""

    NOT_SUSPENDED = 1
    NO_CONTACT = 2


class DeceasedStatus(messages.Enum):
    """Whether the participant has an approved or pending deceased report"""

    UNSET = 0
    PENDING = 1
    APPROVED = 2


class DeceasedNotification(messages.Enum):
    """How the program was notified of the participant's deceased status"""

    EHR = 1
    ATTEMPTED_CONTACT = 2
    NEXT_KIN_HPO = 3
    NEXT_KIN_SUPPORT = 4
    OTHER = 5


class DeceasedReportStatus(messages.Enum):
    """The approval state of the deceased report"""

    PENDING = 1
    APPROVED = 2
    DENIED = 3


class DeceasedReportDenialReason(messages.Enum):
    """The reason that the deceased report was denied"""

    INCORRECT_PARTICIPANT = 1
    MARKED_IN_ERROR = 2
    INSUFFICIENT_INFORMATION = 3
    OTHER = 4


# DA-1576:  Retention Eligibility Metrics
class RetentionStatus(messages.Enum):
    """Whether a participant meets retention-eligible criteria"""

    NOT_ELIGIBLE = 1
    ELIGIBLE = 2


class RetentionType(messages.Enum):

    UNSET = 0
    ACTIVE = 1
    PASSIVE = 2
    ACTIVE_AND_PASSIVE = 3


class MetricsCacheType(messages.Enum):
    """Types of metrics cache"""

    METRICS_V2_API = 0
    PUBLIC_METRICS_EXPORT_API = 1


class MetricsCronJobStage(messages.Enum):
    """stage of metrics cron job"""

    STAGE_ONE = 1
    STAGE_TWO = 2


class OnSiteVerificationType(messages.Enum):
    """Types of on site verification"""
    UNSET = 0
    PHOTO_AND_ONE_OF_PII = 1
    TWO_OF_PII = 2


class OnSiteVerificationVisitType(messages.Enum):
    """Types of on site visit"""
    UNSET = 0
    PMB_INITIAL_VISIT = 1
    PHYSICAL_MEASUREMENTS_ONLY = 2
    BIOSPECIMEN_COLLECTION_ONLY = 3
    BIOSPECIMEN_REDRAW_ONLY = 4
    RETENTION_ACTIVITIES = 5


# M2API age buckets
AGE_BUCKETS_METRICS_V2_API = ["0-17", "18-25", "26-35", "36-45", "46-55", "56-65", "66-75", "76-85", "86-"]
AGE_BUCKETS_PUBLIC_METRICS_EXPORT_API = ["18-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89", "90-"]


class MetricsAPIVersion(messages.Enum):
    """M2API version history"""

    V1 = 1  # M2API version 1
    V2 = 2  # change participant status to 4 tiers


# The lower bounds of the age buckets.
_AGE_LB = [0, 18, 25, 35, 45, 55, 65, 75, 85]
AGE_BUCKETS = ["{}-{}".format(b, e) for b, e in zip(_AGE_LB, [a - 1 for a in _AGE_LB[1:]] + [""])]


def get_bucketed_age(date_of_birth, today):
    if not date_of_birth:
        return UNSET
    age = relativedelta(today, date_of_birth).years
    for begin, end in zip(_AGE_LB, [age_lb - 1 for age_lb in _AGE_LB[1:]] + [""]):
        if (age >= begin) and (not end or age <= end):
            return str(begin) + "-" + str(end)


def _map_single_race(code):
    if code is None:
        return Race.UNSET
    race_value = ANSWER_CODE_TO_RACE.get(code.value)
    if race_value:
        return race_value
    return ANSWER_CODE_TO_RACE.get(code.parent)


def get_race(race_codes):
    """Transforms one or more race codes from questionnaire response answers about race
  into a single race enum; the enum includes values for multiple races.
  See: https://docs.google.com/document/d/1Z1rGULWVlmSIAO38ACjMnz0aMuua3sKqFZXjGqw3gqQ"""
    if not race_codes:
        return None
    if len(race_codes) == 1:
        return _map_single_race(race_codes[0])
    else:
        all_races = set([_map_single_race(race_code) for race_code in race_codes])
        if Race.HISPANIC_LATINO_OR_SPANISH in all_races:
            if len(all_races) > 2:
                return Race.HLS_AND_MORE_THAN_ONE_OTHER_RACE
            if Race.WHITE in all_races:
                return Race.HLS_AND_WHITE
            if Race.BLACK_OR_AFRICAN_AMERICAN in all_races:
                return Race.HLS_AND_BLACK
            return Race.HLS_AND_ONE_OTHER_RACE
        else:
            return Race.MORE_THAN_ONE_RACE


def map_single_gender(code):
    if code is None:
        return
    gender_value = ANSWER_CODE_TO_GENDER.get(code.value)
    if gender_value:
        return gender_value
    return ANSWER_CODE_TO_GENDER.get(code.parent)


def get_gender_identity(gender_codes):
    if not gender_codes:
        return None
    if len(gender_codes) == 1:
        return map_single_gender(gender_codes[0])
    else:
        multiple_genders = set([map_single_gender(gender_code) for gender_code in gender_codes])
        if len(multiple_genders.difference([GenderIdentity.PMI_PreferNotToAnswer, GenderIdentity.PMI_Skip])) == 0:
            return GenderIdentity.PMI_PreferNotToAnswer
        # ignore pmi_prefer_not_to_answer and pmi_skip if in set with more values
        for i in [GenderIdentity.PMI_PreferNotToAnswer, GenderIdentity.PMI_Skip]:
            if i in multiple_genders:
                multiple_genders.remove(i)
        if len(multiple_genders) > 1:
            return GenderIdentity.GenderIdentity_MoreThanOne
        else:
            # you can't have your cake and eat it too
            gender = multiple_genders.pop()
            for code in gender_codes:
                if gender.name == code.value:
                    return map_single_gender(code)
            # return map_single_gender(multiple_genders)


def make_primary_provider_link_for_id(hpo_id):
    from rdr_service.dao.hpo_dao import HPODao

    return make_primary_provider_link_for_hpo(HPODao().get(hpo_id))


def make_primary_provider_link_for_hpo(hpo):
    return make_primary_provider_link_for_name(hpo.name)


def make_primary_provider_link_for_name(hpo_name):
    """Returns serialized FHIR JSON for a provider link based on HPO information.

  The returned JSON represents a list containing the one primary provider.
  """
    return json.dumps([{"primary": True, "organization": {"reference": "Organization/%s" % hpo_name}}], sort_keys=True)


class ParticipantSummaryRecord(messages.Enum):
    NOT_IN_USE = 0
    IN_USE = 1


class WorkbenchWorkspaceStatus(messages.Enum):
    """Status of Workbench Workspace"""

    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class WorkbenchWorkspaceRaceEthnicity(messages.Enum):
    UNSET = 0
    AIAN = 1
    ASIAN = 2
    HISPANIC = 3
    NHPI = 4
    MENA = 5
    AA = 6
    MULTI = 7


class WorkbenchWorkspaceAge(messages.Enum):
    UNSET = 0
    AGE_0_11 = 1
    AGE_12_17 = 2
    AGE_65_74 = 3
    AGE_75_AND_MORE = 4


class WorkbenchWorkspaceSexAtBirth(messages.Enum):
    UNSET = 0
    INTERSEX = 1


class WorkbenchWorkspaceGenderIdentity(messages.Enum):
    UNSET = 0
    OTHER_THAN_MAN_WOMAN = 1


class WorkbenchWorkspaceSexualOrientation(messages.Enum):
    UNSET = 0
    OTHER_THAN_STRAIGHT = 1


class WorkbenchWorkspaceGeography(messages.Enum):
    UNSET = 0
    RURAL = 1


class WorkbenchWorkspaceDisabilityStatus(messages.Enum):
    UNSET = 0
    DISABILITY = 1


class WorkbenchWorkspaceAccessToCare(messages.Enum):
    UNSET = 0
    NOT_EASILY_ACCESS_CARE = 1


class WorkbenchWorkspaceEducationLevel(messages.Enum):
    UNSET = 0
    LESS_THAN_HIGH_SCHOOL = 1


class WorkbenchWorkspaceIncomeLevel(messages.Enum):
    UNSET = 0
    BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT = 1


class WorkbenchWorkspaceAccessTier(messages.Enum):
    UNSET = 0
    REGISTERED = 1
    CONTROLLED = 2


class WorkbenchWorkspaceUserRole(messages.Enum):
    """Status of Workbench Workspace User Role"""

    UNSET = 0
    READER = 1
    WRITER = 2
    OWNER = 3


class WorkbenchInstitutionNonAcademic(messages.Enum):
    """Workbench Institution enum"""

    UNSET = 0
    INDUSTRY = 1
    EDUCATIONAL_INSTITUTION = 2
    CITIZEN_SCIENTIST = 3
    HEALTH_CENTER_OR_NON_PROFIT = 4
    FREE_TEXT = 5


class WorkbenchResearcherEthnicity(messages.Enum):
    UNSET = 0
    HISPANIC = 1
    NOT_HISPANIC = 2
    PREFER_NOT_TO_ANSWER = 3


class WorkbenchResearcherGender(messages.Enum):
    UNSET = 0
    MAN = 1
    WOMAN = 2
    NON_BINARY = 3
    TRANSGENDER = 4
    NONE_DESCRIBE_ME = 5
    PREFER_NOT_TO_ANSWER = 6


class WorkbenchResearcherRace(messages.Enum):
    UNSET = 0
    AIAN = 1
    ASIAN = 2
    AA = 3
    NHOPI = 4
    WHITE = 5
    NONE = 6
    PREFER_NOT_TO_ANSWER = 7


class WorkbenchResearcherSexAtBirth(messages.Enum):
    UNSET = 0
    FEMALE = 1
    MALE = 2
    INTERSEX = 3
    NONE_OF_THESE_DESCRIBE_ME = 4
    PREFER_NOT_TO_ANSWER = 5


class WorkbenchResearcherSexualOrientation(messages.Enum):
    UNSET = 0
    STRAIGHT = 1
    GAY = 2
    LESBIAN = 3
    BISEXUAL = 4
    NONE_OF_THESE_DESCRIBE_ME = 5
    PREFER_NOT_TO_ANSWER = 6


class WorkbenchResearcherEducation(messages.Enum):
    UNSET = 0
    NO_EDUCATION = 1
    GRADES_1_12 = 2
    COLLEGE_GRADUATE = 3
    UNDERGRADUATE = 4
    MASTER = 5
    DOCTORATE = 6
    PREFER_NOT_TO_ANSWER = 7


class WorkbenchResearcherDegree(messages.Enum):
    UNSET = 0
    PHD = 1
    MD = 2
    JD = 3
    EDD = 4
    MSN = 5
    MS = 6
    MA = 7
    MBA = 8
    ME = 9
    BA = 10
    BS = 11
    BSN = 12
    MSW = 13
    MPH = 14


class WorkbenchResearcherAccessTierShortName(messages.Enum):
    REGISTERED = 1
    CONTROLLED = 2


class WorkbenchResearcherDisability(messages.Enum):
    UNSET = 0
    YES = 1
    NO = 2
    PREFER_NOT_TO_ANSWER = 3


class WorkbenchAuditReviewType(messages.Enum):
    UNSET = 0
    INITIAL = 1
    SECOND = 2
    RAB = 3


class WorkbenchAuditWorkspaceDisplayDecision(messages.Enum):
    UNSET = 0
    PUBLISH_TO_RESEARCHER_DIRECTORY = 1
    EXCLUDE_FROM_RESEARCHER_DIRECTORY = 2


class WorkbenchAuditWorkspaceAccessDecision(messages.Enum):
    UNSET = 0
    DISABLE_WORKSPACE = 1
    DISABLE_WORKSPACE_AND_REVIEW_RESEARCHERS = 2


class CdrEtlSurveyStatus(messages.Enum):
    EXCLUDE = 1
    INCLUDE = 2


class CdrEtlCodeType(messages.Enum):
    MODULE = 1
    QUESTION = 2
    ANSWER = 3


class WorkbenchResearcherEthnicCategory(messages.Enum):
    AI_AN = 1
    AI_AN_CENTRAL_SOUTH = 2
    AI_AN_OTHER = 3
    ASIAN = 4
    ASIAN_INDIAN = 5
    ASIAN_CAMBODIAN = 6
    ASIAN_CHINESE = 7
    ASIAN_FILIPINO = 8
    ASIAN_HMONG = 9
    ASIAN_JAPANESE = 10
    ASIAN_KOREAN = 11
    ASIAN_LAO = 12
    ASIAN_PAKISTANI = 13
    ASIAN_VIETNAMESE = 14
    ASIAN_OTHER = 15
    BLACK = 16
    HISPANIC = 17
    MENA = 18
    NHPI = 19
    WHITE = 20
    OTHER = 21
    PREFER_NOT_TO_ANSWER = 22

    # New subcategories
    BLACK_AA = 23
    BLACK_BARBADIAN = 24
    BLACK_CARIBBEAN = 25
    BLACK_ETHIOPIAN = 26
    BLACK_GHANAIAN = 27
    BLACK_HAITIAN = 28
    BLACK_JAMAICAN = 29
    BLACK_LIBERIAN = 30
    BLACK_NIGERIAN = 31
    BLACK_SOMALI = 32
    BLACK_SOUTH_AFRICAN = 33
    BLACK_OTHER = 34
    HISPANIC_COLOMBIAN = 35
    HISPANIC_CUBAN = 36
    HISPANIC_DOMINICAN = 37
    HISPANIC_ECUADORIAN = 38
    HISPANIC_HONDURAN = 39
    HISPANIC_MEXICAN = 40
    HISPANIC_PUERTO_RICAN = 41
    HISPANIC_SALVADORAN = 42
    HISPANIC_SPANISH = 43
    HISPANIC_OTHER = 44
    MENA_AFGHAN = 45
    MENA_ALGERIAN = 46
    MENA_EGYPTIAN = 47
    MENA_IRANIAN = 48
    MENA_IRAQI = 49
    MENA_ISRAELI = 50
    MENA_LEBANESE = 51
    MENA_MOROCCAN = 52
    MENA_SYRIAN = 53
    MENA_TUNISIAN = 54
    MENA_OTHER = 55
    NHPI_CHAMORRO = 56
    NHPI_CHUUKESE = 57
    NHPI_FIJIAN = 58
    NHPI_MARSHALLESE = 59
    NHPI_HAWAIIAN = 60
    NHPI_PALAUAN = 61
    NHPI_SAMOAN = 62
    NHPI_TAHITIAN = 63
    NHPI_TONGAN = 64
    NHPI_OTHER = 65
    WHITE_DUTCH = 66
    WHITE_ENGLISH = 67
    WHITE_EUROPEAN = 68
    WHITE_FRENCH = 69
    WHITE_GERMAN = 70
    WHITE_IRISH = 71
    WHITE_ITALIAN = 72
    WHITE_NORWEGIAN = 73
    WHITE_POLISH = 74
    WHITE_SCOTTISH = 75
    WHITE_SPANISH = 76
    WHITE_OTHER = 77
    AI_AN_AMERICAN_INDIAN = 78
    AI_AN_ALASKA_NATIVE = 79
    BLACK_SOUTH_AFRICAN = 80
    MENA_LEBANESE = 81


class WorkbenchResearcherGenderIdentity(messages.Enum):
    GENDERQUEER = 1
    MAN = 2
    NON_BINARY = 3
    QUESTIONING = 4
    TRANS_MAN = 5
    TRANS_WOMAN = 6
    TWO_SPIRIT = 7
    WOMAN = 8
    OTHER = 9
    PREFER_NOT_TO_ANSWER = 10


class WorkbenchResearcherSexualOrientationV2(messages.Enum):
    ASEXUAL = 1
    BISEXUAL = 2
    GAY = 3
    LESBIAN = 4
    POLYSEXUAL = 5
    QUEER = 6
    QUESTIONING = 7
    SAME_GENDER = 8
    STRAIGHT = 9
    TWO_SPIRIT = 10
    OTHER = 11
    PREFER_NOT_TO_ANSWER = 12


class WorkbenchResearcherSexAtBirthV2(messages.Enum):
    UNSET = 0
    FEMALE = 1
    INTERSEX = 2
    MALE = 3
    OTHER = 4
    PREFER_NOT_TO_ANSWER = 5


class WorkbenchResearcherEducationV2(messages.Enum):
    UNSET = 0
    NO_EDUCATION = 1
    GRADES_1_12 = 2
    UNDERGRADUATE = 3
    COLLEGE_GRADUATE = 4
    MASTER = 5
    DOCTORATE = 6
    PREFER_NOT_TO_ANSWER = 7


class WorkbenchResearcherYesNoPreferNot(messages.Enum):
    UNSET = 0
    YES = 1
    NO = 2
    PREFER_NOT_TO_ANSWER = 3
