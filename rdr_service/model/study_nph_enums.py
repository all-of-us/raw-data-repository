from protorpc import messages


class StoredSampleStatus(messages.Enum):
    """Status of NPH StoredSample"""

    RECEIVED = 0
    SHIPPED = 1
    DISPOSED = 2
