import json

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
TEST_HPO_ID = 19
# Test login phone number prefix
TEST_LOGIN_PHONE_NUMBER_PREFIX = "444"


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


class EnrollmentStatus(messages.Enum):
    """A status reflecting how fully enrolled a participant is"""

    INTERESTED = 1
    MEMBER = 2
    FULL_PARTICIPANT = 3


class EnrollmentStatusV2(messages.Enum):
    """A status reflecting how fully enrolled a participant is(Version 2)"""

    REGISTERED = 0
    PARTICIPANT = 1
    FULLY_CONSENTED = 2
    CORE_PARTICIPANT = 3


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


class WithdrawalReason(messages.Enum):
    """Whether a participant has been administratively withdrawn from the study.
     If value is UNSET it mean that a participant went through the normal withdrawal process."""

    UNSET = 0
    FRAUDULENT = 1
    DUPLICATE = 2
    TEST = 3


class SuspensionStatus(messages.Enum):
    """Whether a participant has been suspended from the study."""

    NOT_SUSPENDED = 1
    NO_CONTACT = 2


class MetricsCacheType(messages.Enum):
    """Types of metrics cache"""

    METRICS_V2_API = 0
    PUBLIC_METRICS_EXPORT_API = 1


# M2API age buckets
AGE_BUCKETS_METRICS_V2_API = ["0-17", "18-25", "26-35", "36-45", "46-55", "56-65", "66-75", "76-85", "86-"]
AGE_BUCKETS_PUBLIC_METRICS_EXPORT_API = ["18-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89", "90-"]


class MetricsAPIVersion(messages.Enum):
    """M2API version history"""

    V1 = 1  # M2API version 1
    V2 = 2  # change participant status to 4 tiers


# The lower bounds of the age buckets.
_AGE_LB = [0, 18, 26, 36, 46, 56, 66, 76, 86]
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
