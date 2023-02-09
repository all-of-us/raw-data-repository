from protorpc import messages

class SiteStatus(messages.Enum):
    """ The active scheduling status of a site. """

    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class EnrollingStatus(messages.Enum):
    """ The actively enrolling status of a site. """

    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class DigitalSchedulingStatus(messages.Enum):
    """ The status of a sites digital scheduling capability. """

    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2

# DA-3300: Add support for the status-for-in-person-operations FHIR extension in SITE update payloads
class InPersonOperationsStatus(messages.Enum):
    """ The in-person operations status of a site """
    # Based on a drop-down list of values from PMT
    UNSET = 0
    ONBOARDING = 1
    APPROVED_TO_OPEN = 2
    OPEN_ENGAGEMENT_RECRUITMENT_ENROLLMENT = 3
    OPEN_ENGAGEMENT_ONLY = 4
    PAUSED = 5
    CLOSED_TEMPORARILY = 6
    CLOSED_PERMANENTLY = 7
    ERROR_NEVER_ACTIVATED = 8
    NOT_APPLICABLE_VIRTUAL_SITE = 9


# Mapping potential valueString options (from PMT drop-down) for status-for-in-person-operations extension
IN_PERSON_STATUS_OPTIONS = {
    'onboarding': InPersonOperationsStatus.ONBOARDING,
    'approved to open': InPersonOperationsStatus.APPROVED_TO_OPEN,
    'open - engagement, recruitment, & enrollment': InPersonOperationsStatus.OPEN_ENGAGEMENT_RECRUITMENT_ENROLLMENT,
    'open - engagement only': InPersonOperationsStatus.OPEN_ENGAGEMENT_ONLY,
    'paused': InPersonOperationsStatus.PAUSED,
    'closed temporarily': InPersonOperationsStatus.CLOSED_TEMPORARILY,
    'closed permanently': InPersonOperationsStatus.CLOSED_PERMANENTLY,
    'error/never activated': InPersonOperationsStatus.ERROR_NEVER_ACTIVATED,
    'not applicable/virtual site type': InPersonOperationsStatus.NOT_APPLICABLE_VIRTUAL_SITE
}


class ObsoleteStatus(messages.Enum):
    """ If an organization is obsolete but referenced in other tables. """

    ACTIVE = 0
    OBSOLETE = 1
