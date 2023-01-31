from protorpc import messages


class StoredSampleStatus(messages.Enum):
    """Status of Genomic Set"""

    RECEIVED = 0
    SHIPPED = 1
    DISPOSED = 2
