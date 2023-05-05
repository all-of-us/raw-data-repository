from protorpc import messages


class StoredSampleStatus(messages.Enum):
    """Status of NPH StoredSample"""

    UNSET = 0
    RECEIVED = 1
    SHIPPED = 2
    DISPOSED = 3


class IncidentStatus(messages.Enum):
    """Status of NPH"""
    OPEN = 0
    RESOLVED = 1
    UNABLE_TO_RESOLVE = 2


class IncidentType(messages.Enum):
    UNSET = 0
