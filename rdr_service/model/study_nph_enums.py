from protorpc import messages


class StoredSampleStatus(messages.Enum):
    """Status of NPH StoredSample"""

    RECEIVED = 0
    SHIPPED = 1
    DISPOSED = 2


class IncidentStatus(messages.Enum):
    """Status of NPH"""
    OPEN = 0
    RESOLVED = 1
    UNABLE_TO_RESOLVE = 2


class IncidentType(messages.Enum):
    UNSET = 0
