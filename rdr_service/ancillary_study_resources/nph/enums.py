from protorpc import messages


class Activity(messages.Enum):
    """TODO: Synced with nph.activity table"""

    ENROLLMENT = 1
    PAIRING = 2
    CONSENT = 3


class EnrollmentEventTypes(messages.Enum):
    """TODO: Synced with nph.enrollment_event_type table"""

    REFERRED = 1
    MODULE_1_CONSENTED = 2
    MODULE_1_ELIGIBILITY_CONFIRMED = 3
    MODULE_1_ELIGIBILITY_FAILED = 4
    MODULE_1_STARTED = 5
    MODULE_1_COMPLETE = 6
    MODULE_2_CONSENTED = 7
    MODULE_2_ELIGIBILITY_CONFIRMED = 8
    MODULE_2_ELIGIBILITY_FAILED = 9
    MODULE_2_STARTED = 10
    MODULE_2_DIET_ASSIGNED = 11
    MODULE_2_COMPLETE = 12
    MODULE_3_CONSENTED = 13
    MODULE_3_ELIGIBILITY_CONFIRMED = 14
    MODULE_3_ELIGIBILITY_FAILED = 15
    MODULE_3_STARTED = 16
    MODULE_3_DIET_ASSIGNED = 17
    MODULE_3_COMPLETE = 18
    WITHDRAWN = 19
    DEACTIVATED = 20


class ConsentOptInTypes(messages.Enum):

    PERMIT = 1
    DENY = 2
    PERMIT2 = 3
    PERMIT3 = 4


class ParticipantOpsElementTypes(messages.Enum):

    BIRTHDATE = 1


class StoredSampleStatus(messages.Enum):
    """Status of NPH StoredSample"""

    UNSET = 0
    RECEIVED = 1
    SHIPPED = 2
    DISPOSED = 3


class ModuleTypes(messages.Enum):

    MODULE1 = 1
    MODULE2 = 2
    MODULE3 = 3


class IncidentStatus(messages.Enum):
    """Status of NPH"""
    OPEN = 0
    RESOLVED = 1
    UNABLE_TO_RESOLVE = 2


class IncidentType(messages.Enum):

    UNSET = 0


class DietType(messages.Enum):

    BLUE = 1
    ORANGE = 2
    PURPLE = 3
    LMT = 4


class DietStatus(messages.Enum):

    STARTED = 1
    COMPLETED = 2
    CONTINUED = 3
    DISCONTINUED = 4

