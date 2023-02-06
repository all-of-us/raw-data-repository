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


class ObsoleteStatus(messages.Enum):
    """ If an organization is obsolete but referenced in other tables. """

    ACTIVE = 0
    OBSOLETE = 1
